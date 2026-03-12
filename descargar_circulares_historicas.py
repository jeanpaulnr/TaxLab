# -*- coding: utf-8 -*-
"""
TaxLab - Descargador de Circulares Historicas (pre-2013)
=======================================================
Descarga circulares desde https://www.sii.cl/documentos/circulares/
que es la ruta antigua del SII, diferente de normativa_legislacion/.

Pipeline por cada circular:
1. Intentar descargar PDF primero
2. Si no hay PDF, descargar HTML
3. Si es HTML: detectar encoding, reparar mojibake, limpiar DOM, extraer texto
4. Si es DOC: intentar extraer texto con antiword, LibreOffice o Word COM
5. Si el HTML es imagen escaneada: seguir paginas relacionadas, OCR de imagenes y PDF local
6. Guardar en DB con fuente='scraper_historico'
7. Guardar HTML original + HTML limpio como archivos locales
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
import textwrap
from collections import OrderedDict
from urllib.parse import urljoin, urlparse

import charset_normalizer
from bs4 import BeautifulSoup, Comment
from PIL import Image, ImageChops, ImageFilter, ImageOps
import fitz
from pdf_layout import PDF_ROOT

try:
    from rapidocr_onnxruntime import RapidOCR
except Exception:
    RapidOCR = None

from scraper.engine import (
    SESSION,
    detectar_articulos,
    detectar_leyes,
    doc_existe,
    doc_existe_hash,
    extraer_fecha_texto,
    extraer_resumen,
    extraer_texto_pdf,
    guardar_documento,
    log,
    log_scraper,
)

BASE = os.path.dirname(os.path.abspath(__file__))
PDF_HIST_DIR = os.path.join(PDF_ROOT, 'circular')
HTML_DIR = os.path.join(BASE, 'html_historico')
DOC_DIR = os.path.join(BASE, 'doc_historico')
IMG_DIR = os.path.join(BASE, 'img_historico')
OCR_DIR = os.path.join(BASE, 'ocr_historico')
os.makedirs(PDF_HIST_DIR, exist_ok=True)
os.makedirs(HTML_DIR, exist_ok=True)
os.makedirs(DOC_DIR, exist_ok=True)
os.makedirs(IMG_DIR, exist_ok=True)
os.makedirs(OCR_DIR, exist_ok=True)

BASE_URL = 'https://www.sii.cl/documentos/circulares'
LINK_RE = re.compile(r'(?i)^(?:circu\d+[a-z]?|\d+[a-z]?)\.(?:htm|html|pdf|doc)$')
TITLE_RE = re.compile(r'circular\s*n(?:\D{0,3})\s*0*(\d+[a-z]?)', re.IGNORECASE)
IMAGE_EXT_RE = re.compile(r'(?i)\.(gif|png|jpg|jpeg|bmp|tif|tiff)$')

REPARACIONES = {
    '\u00c3\u00a1': '\u00e1', '\u00c3\u00a9': '\u00e9', '\u00c3\u00ad': '\u00ed', '\u00c3\u00b3': '\u00f3', '\u00c3\u00ba': '\u00fa',
    '\u00c3\u0081': '\u00c1', '\u00c3\u0089': '\u00c9', '\u00c3\u008d': '\u00cd', '\u00c3\u201c': '\u00d3', '\u00c3\u0161': '\u00da',
    '\u00c3\u00b1': '\u00f1', '\u00c3\u2018': '\u00d1', '&nbsp;': ' ', '&#160;': ' ',
    '\x92': "'", '\x93': '"', '\x94': '"', '\x96': '-', '\x97': '-',
    'APLICACI?N': 'APLICACION', 'ART?CULO': 'ARTICULO', 'OBLIGACI?N': 'OBLIGACION',
}

_RAPID_OCR = None


def safe_name(value):
    value = re.sub(r'[^0-9A-Za-z._-]+', '_', value or '')
    return value.strip('_') or 'sin_nombre'


def ensure_year_dir(root, anio):
    path = os.path.join(root, str(anio))
    os.makedirs(path, exist_ok=True)
    return path


def urls_indice(anio):
    base = f'{BASE_URL}/{anio}'
    candidatos = [
        f'{base}/indcir{anio}.htm',
        f'{base}/indcir{anio}.html',
    ]
    if anio < 2000:
        yy = str(anio)[-2:]
        candidatos.extend([
            f'{base}/indcir{yy}.htm',
            f'{base}/indcir{yy}.html',
        ])
    vistos = []
    for url in candidatos:
        if url not in vistos:
            vistos.append(url)
    return vistos


def reparar_mojibake(texto):
    limpio = texto
    for malo, bueno in REPARACIONES.items():
        limpio = limpio.replace(malo, bueno)
    return limpio


def decodificar_html(raw_bytes):
    deteccion = charset_normalizer.from_bytes(raw_bytes).best()
    if deteccion:
        encoding = deteccion.encoding or 'desconocido'
        texto = str(deteccion)
    else:
        try:
            texto = raw_bytes.decode('windows-1252')
            encoding = 'windows-1252'
        except Exception:
            texto = raw_bytes.decode('utf-8', errors='replace')
            encoding = 'utf-8-fallback'
    return reparar_mojibake(texto), encoding


def limpiar_texto(texto):
    return re.sub(r'\s+', ' ', (texto or '')).strip()


def extraer_numero(titulo, base):
    titulo = titulo or ''
    m = TITLE_RE.search(titulo)
    if m:
        return m.group(1)
    m = re.search(r'circu0*(\d+[a-z]?)$', base, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r'^(\d+[a-z]?)$', base, re.IGNORECASE)
    if m:
        return m.group(1)
    return base


def raiz_documento(base):
    m = re.match(r'^(.*?\d+)([a-z]?)$', base, re.IGNORECASE)
    return m.group(1) if m else base


def ordenar_pagina(url, root):
    base = os.path.splitext(os.path.basename(urlparse(url).path))[0]
    sufijo = ''
    if base.lower().startswith(root.lower()):
        sufijo = base[len(root):].lower()
    return (0 if sufijo == '' else 1, sufijo)


def descargar_indice(anio):
    for url in urls_indice(anio):
        try:
            r = SESSION.get(url, timeout=15)
            if r.status_code == 200 and len(r.content) > 300:
                html, encoding = decodificar_html(r.content)
                return url, html, encoding
        except Exception as e:
            log.warning(f'[historico] indice {anio} fallido {url}: {e}')
    return None, None, None


def descubrir_circulares(anio, index_url, html):
    soup = BeautifulSoup(html, 'lxml')
    docs = OrderedDict()

    for a in soup.find_all('a', href=True):
        href = (a.get('href') or '').strip()
        if not href or href.startswith('#'):
            continue
        abs_url = urljoin(index_url, href)
        basename = os.path.basename(urlparse(abs_url).path)
        if not LINK_RE.match(basename):
            continue

        base, ext = os.path.splitext(basename)
        titulo = limpiar_texto(a.get_text(' ', strip=True))
        contexto = limpiar_texto(a.parent.get_text(' ', strip=True)) if a.parent else titulo
        numero = extraer_numero(titulo or contexto, base)

        key = base.lower()
        item = docs.get(key) or {
            'anio': anio,
            'numero': numero,
            'base': base,
            'nombre_archivo': basename,
            'titulo': titulo or contexto or f'Circular {base}',
            'descripcion': contexto[:600],
            'href_original': abs_url,
            'extensiones': set(),
        }
        item['extensiones'].add(ext.lower())
        if titulo and len(titulo) > len(item.get('titulo', '')):
            item['titulo'] = titulo
        if contexto and len(contexto) > len(item.get('descripcion', '')):
            item['descripcion'] = contexto[:600]
        docs[key] = item

    resultado = []
    for item in docs.values():
        item['extensiones'] = sorted(item['extensiones'])
        resultado.append(item)
    return resultado


def guardar_htmls(nombre, anio, raw_bytes, html_limpio):
    year_dir = ensure_year_dir(HTML_DIR, anio)
    stem = safe_name(f'{anio}_{nombre}')
    original = os.path.join(year_dir, f'{stem}_original.html')
    limpio = os.path.join(year_dir, f'{stem}_limpio.html')
    with open(original, 'wb') as f:
        f.write(raw_bytes)
    with open(limpio, 'w', encoding='utf-8') as f:
        f.write(html_limpio)
    return os.path.relpath(original, BASE), os.path.relpath(limpio, BASE)


def guardar_doc(nombre, anio, raw_bytes):
    year_dir = ensure_year_dir(DOC_DIR, anio)
    stem = safe_name(f'{anio}_{nombre}')
    path = os.path.join(year_dir, f'{stem}.doc')
    with open(path, 'wb') as f:
        f.write(raw_bytes)
    return path, os.path.relpath(path, BASE)


def guardar_imagen(nombre, anio, idx, img_url, raw_bytes):
    year_dir = ensure_year_dir(IMG_DIR, anio)
    ext = os.path.splitext(os.path.basename(urlparse(img_url).path))[1].lower() or '.img'
    stem = safe_name(f'{anio}_{nombre}_{idx:02d}')
    path = os.path.join(year_dir, f'{stem}{ext}')
    with open(path, 'wb') as f:
        f.write(raw_bytes)
    return path, os.path.relpath(path, BASE)


def es_linea_ruido_historico(linea):
    linea = limpiar_texto(linea)
    if not linea:
        return True
    if linea.startswith('Home |'):
        return True
    if re.fullmatch(r'(?:\[\d+\],?\s*)+', linea):
        return True
    if re.fullmatch(r'circulares\s+\d{4}', linea, re.IGNORECASE):
        return True
    return False


def deduplicar_lineas(lineas):
    resultado = []
    anterior = None
    for linea in lineas:
        if not linea:
            continue
        if linea == anterior:
            continue
        resultado.append(linea)
        anterior = linea
    return resultado


def extraer_metadata_html(soup, fallback_title=''):
    title_tag = soup.find('title')
    title_text = limpiar_texto(reparar_mojibake(title_tag.get_text(' ', strip=True))) if title_tag else ''
    meta = soup.find('meta', attrs={'name': re.compile(r'^description$', re.I)})
    meta_text = limpiar_texto(reparar_mojibake(meta.get('content', ''))) if meta else ''

    titulo = normalizar_texto_ocr(title_text or fallback_title or '')
    materia = normalizar_texto_ocr(meta_text)
    if materia.upper().startswith('MATERIA:'):
        materia = limpiar_texto(materia.split(':', 1)[1])
    return titulo, materia


def extraer_texto_html(soup, titulo='', materia=''):
    soup = BeautifulSoup(str(soup), 'lxml')

    for comentario in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comentario.extract()

    for tag in soup.find_all(['script', 'style', 'meta', 'link']):
        tag.decompose()

    for br in soup.find_all('br'):
        br.replace_with('\n')

    body = soup.body or soup
    texto = body.get_text(separator='\n', strip=True)
    lineas = []
    titulo_norm = limpiar_texto(normalizar_texto_ocr(titulo or ''))
    materia_norm = limpiar_texto(normalizar_texto_ocr(materia or ''))

    for raw in texto.splitlines():
        linea = limpiar_texto(normalizar_texto_ocr(raw))
        if es_linea_ruido_historico(linea):
            continue
        lineas.append(linea)

    if titulo_norm:
        lineas = [l for l in lineas if l != titulo_norm]
    if materia_norm:
        lineas = [l for l in lineas if l != f'MATERIA: {materia_norm}' and l != materia_norm]

    lineas = deduplicar_lineas(lineas)
    texto_limpio = '\n'.join(lineas)
    texto_limpio = re.sub(r'\n{3,}', '\n\n', texto_limpio)
    texto_limpio = re.sub(r'[ \t]+', ' ', texto_limpio)
    return texto_limpio.strip()

def leer_texto_archivo(path):
    for encoding in ('utf-8', 'utf-16', 'utf-16-le', 'utf-16-be', 'windows-1252', 'latin-1'):
        try:
            with open(path, 'r', encoding=encoding) as f:
                texto = f.read()
            return reparar_mojibake(texto)
        except Exception:
            continue
    return None


def descargar_recurso(url, timeout=20):
    try:
        r = SESSION.get(url, timeout=timeout)
        if r.status_code == 200 and len(r.content) > 100:
            return r
    except Exception as e:
        log.warning(f'[historico] error recurso {url}: {e}')
    return None


def extraer_doc_con_antiword(doc_path):
    if not shutil.which('antiword'):
        return None
    try:
        proc = subprocess.run(
            ['antiword', doc_path],
            capture_output=True,
            timeout=120,
            check=False,
        )
        if proc.returncode != 0:
            return None
        return decodificar_html(proc.stdout)[0]
    except Exception:
        return None


def extraer_doc_con_soffice(doc_path):
    exe = shutil.which('soffice')
    if not exe:
        return None

    tmpdir = tempfile.mkdtemp(prefix='taxlab_doc_')
    try:
        proc = subprocess.run(
            [exe, '--headless', '--convert-to', 'txt:Text', '--outdir', tmpdir, doc_path],
            capture_output=True,
            timeout=180,
            check=False,
        )
        if proc.returncode != 0:
            return None

        base = os.path.splitext(os.path.basename(doc_path))[0]
        txt_path = os.path.join(tmpdir, f'{base}.txt')
        if not os.path.exists(txt_path):
            return None
        return leer_texto_archivo(txt_path)
    except Exception:
        return None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def extraer_doc_con_word(doc_path):
    powershell = shutil.which('powershell') or shutil.which('pwsh')
    if not powershell:
        return None

    tmpdir = tempfile.mkdtemp(prefix='taxlab_word_')
    txt_path = os.path.join(tmpdir, 'salida.txt')
    ps_doc = doc_path.replace("'", "''")
    ps_txt = txt_path.replace("'", "''")
    script = f"""
$ErrorActionPreference = 'Stop'
$word = $null
$doc = $null
try {{
    $word = New-Object -ComObject Word.Application
    $word.Visible = $false
    $word.DisplayAlerts = 0
    $doc = $word.Documents.Open('{ps_doc}', $false, $true)
    $text = $doc.Content.Text
    Set-Content -Path '{ps_txt}' -Value $text -Encoding UTF8
}} finally {{
    if ($doc -ne $null) {{ $doc.Close($false) }}
    if ($word -ne $null) {{ $word.Quit() }}
}}
"""
    try:
        proc = subprocess.run(
            [powershell, '-NoProfile', '-ExecutionPolicy', 'Bypass', '-Command', script],
            capture_output=True,
            timeout=180,
            check=False,
        )
        if proc.returncode != 0 or not os.path.exists(txt_path):
            return None
        return leer_texto_archivo(txt_path)
    except Exception:
        return None
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def get_rapid_ocr():
    global _RAPID_OCR
    if RapidOCR is None:
        return None
    if _RAPID_OCR is None:
        _RAPID_OCR = RapidOCR()
    return _RAPID_OCR


def normalizar_texto_ocr(texto):
    texto = reparar_mojibake(texto or '')
    texto = texto.replace('\r', '\n')
    texto = texto.replace('\x0c', '\n')
    texto = re.sub(r'(\w)-\n(\w)', r'\1\2', texto)
    texto = re.sub(r'[ \t]+', ' ', texto)
    texto = re.sub(r' *\n *', '\n', texto)
    texto = re.sub(r'\n{3,}', '\n\n', texto)

    reemplazos = {
        'N? ': 'N\u00ba ',
        'N\u00af ': 'N\u00ba ',
        'N\u00b0 ': 'N\u00ba ',
        'A\u2014O': 'A\u00d1O',
        'A?O': 'A\u00d1O',
        'ART\u00ebCULO': 'ART\u00cdCULO',
        'ARTICULO': 'ART\u00cdCULO',
        'RESOLUCION': 'RESOLUCI\u00d3N',
        'RESOLUCION N\u00ba': 'RESOLUCI\u00d3N N\u00ba',
        'OBLIGACION': 'OBLIGACI\u00d3N',
        'RETENCION': 'RETENCI\u00d3N',
    }
    for malo, bueno in reemplazos.items():
        texto = texto.replace(malo, bueno)

    texto = re.sub(r'\bN[?\u00af\u00b0\u00ba]\s*', 'N\u00ba ', texto)
    texto = re.sub(r'AN[~\u2014?]O', 'A\u00d1O', texto, flags=re.IGNORECASE)
    return texto.strip()


def puntuar_texto_ocr(texto):
    texto = limpiar_texto(texto)
    if not texto:
        return 0
    palabras = re.findall(r'[A-Za-z\u00c0-\u017f0-9]{3,}', texto)
    lineas = [l for l in texto.splitlines() if limpiar_texto(l)]
    score = len(palabras) * 4 + len(lineas)
    score -= texto.count('?') * 2
    score -= texto.count('\ufffd') * 4
    return score


def buscar_tesseract():
    candidatos = [
        shutil.which('tesseract'),
        r'C:\Program Files\Tesseract-OCR\tesseract.exe',
        r'C:\Program Files (x86)\Tesseract-OCR\tesseract.exe',
    ]
    for exe in candidatos:
        if exe and os.path.exists(exe):
            return exe
    return None


def recortar_margenes(img):
    fondo = Image.new(img.mode, img.size, 255)
    diff = ImageChops.difference(img, fondo)
    bbox = diff.getbbox()
    if bbox:
        return img.crop(bbox)
    return img


def preparar_variantes_para_ocr(image_path):
    variantes = [('orig', image_path)]
    temporales = []
    try:
        with Image.open(image_path) as img:
            base = ImageOps.exif_transpose(img).convert('L')
            base = recortar_margenes(base)
            if base.width < 1600:
                scale = max(2, int(1600 / max(base.width, 1)))
                base = base.resize((base.width * scale, base.height * scale), Image.Resampling.LANCZOS)
            auto = ImageOps.autocontrast(base)
            sharpen = auto.filter(ImageFilter.SHARPEN)
            bw = sharpen.point(lambda p: 255 if p > 180 else 0)

            for nombre, imagen in (('gray', auto), ('sharp', sharpen), ('bw', bw)):
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f'_{nombre}.png')
                imagen.save(tmp.name, format='PNG')
                tmp.close()
                variantes.append((nombre, tmp.name))
                temporales.append(tmp.name)
    except Exception:
        pass
    return variantes


def ocr_con_tesseract(image_path, lang='spa+eng', psm='6'):
    exe = buscar_tesseract()
    if not exe:
        return None

    mejor_texto = None
    mejor_score = -1
    variantes = preparar_variantes_para_ocr(image_path)
    umbral_corte = 350
    try:
        for nombre, variante_path in variantes:
            try:
                proc = subprocess.run(
                    [exe, variante_path, 'stdout', '-l', lang, '--psm', psm, '-c', 'preserve_interword_spaces=1'],
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    timeout=25,
                    check=False,
                )
            except Exception:
                continue
            if proc.returncode != 0:
                continue
            texto = proc.stdout or ''
            score = puntuar_texto_ocr(texto)
            if score > mejor_score:
                mejor_score = score
                mejor_texto = texto
            if mejor_score >= umbral_corte and nombre in ('orig', 'gray', 'sharp', 'bw'):
                break
    finally:
        for nombre, variante_path in variantes:
            if nombre == 'orig':
                continue
            try:
                os.remove(variante_path)
            except OSError:
                pass

    return normalizar_texto_ocr(mejor_texto) if mejor_texto else None


def ocr_con_rapidocr(image_path):
    engine = get_rapid_ocr()
    if engine is None:
        return None

    mejor_texto = None
    mejor_score = -1
    variantes = preparar_variantes_para_ocr(image_path)
    try:
        for _, variante_path in variantes:
            try:
                result, _ = engine(variante_path)
                if not result:
                    continue
                lineas = []
                for item in result:
                    if len(item) >= 2:
                        lineas.append(item[1])
                texto = '\n'.join(lineas)
                score = puntuar_texto_ocr(texto)
                if score > mejor_score:
                    mejor_score = score
                    mejor_texto = texto
            except Exception:
                continue
    finally:
        for nombre, variante_path in variantes:
            if nombre == 'orig':
                continue
            try:
                os.remove(variante_path)
            except OSError:
                pass

    return normalizar_texto_ocr(mejor_texto) if mejor_texto else None

def construir_pdf_desde_imagenes(image_paths, anio, nombre):
    imagenes = []
    for path in image_paths:
        with Image.open(path) as img:
            imagenes.append(img.convert('RGB').copy())

    if not imagenes:
        return None

    year_dir = ensure_year_dir(PDF_HIST_DIR, anio)
    pdf_name = safe_name(f'circular_historica_{anio}_{nombre}_scan.pdf')
    pdf_path = os.path.join(year_dir, pdf_name)
    primera, resto = imagenes[0], imagenes[1:]
    primera.save(pdf_path, save_all=True, append_images=resto)
    return os.path.relpath(pdf_path, BASE)



def construir_pdf_desde_texto(texto, anio, nombre, titulo=''):
    if not fitz:
        return None

    year_dir = ensure_year_dir(PDF_HIST_DIR, anio)
    pdf_name = safe_name(f'circular_historica_{anio}_{nombre}_html.pdf')
    pdf_path = os.path.join(year_dir, pdf_name)

    bloques = []
    if titulo:
        bloques.append(titulo)
        bloques.append('')

    texto_base = (texto or '').replace('\r', '')
    for bloque in re.split(r'\n{2,}', texto_base):
        bloque = bloque.strip()
        if not bloque:
            continue
        lineas = textwrap.wrap(bloque, width=95, break_long_words=False, replace_whitespace=False)
        if lineas:
            bloques.extend(lineas)
        else:
            bloques.append(bloque)
        bloques.append('')

    doc = fitz.open()
    width, height = 595, 842
    margin_x = 48
    margin_y = 52
    line_height = 13
    y = margin_y
    page = doc.new_page(width=width, height=height)

    for idx, linea in enumerate(bloques):
        if y > height - margin_y - line_height:
            page = doc.new_page(width=width, height=height)
            y = margin_y
        fontsize = 12 if idx == 0 and titulo else 10
        page.insert_text((margin_x, y), linea, fontsize=fontsize, fontname='helv')
        y += line_height if linea else int(line_height * 0.7)

    doc.save(pdf_path)
    doc.close()
    return os.path.relpath(pdf_path, BASE)

def descubrir_paginas_escaneadas(current_url, html_text):
    soup = BeautifulSoup(html_text, 'lxml')
    current_base = os.path.splitext(os.path.basename(urlparse(current_url).path))[0]
    prefijos = prefijos_documento(current_base)
    sort_root = raiz_documento(current_base).lower()
    paginas = {current_url}

    for a in soup.find_all('a', href=True):
        href = (a.get('href') or '').strip()
        if not href:
            continue
        abs_url = urljoin(current_url, href)
        base = os.path.splitext(os.path.basename(urlparse(abs_url).path))[0]
        ext = os.path.splitext(os.path.basename(urlparse(abs_url).path))[1].lower()
        base_lower = base.lower()
        if ext not in ('.htm', '.html'):
            continue
        if any(re.match(rf'(?i)^{re.escape(prefijo)}[a-z]?$', base_lower) for prefijo in prefijos):
            paginas.add(abs_url)

    return sorted(paginas, key=lambda url: ordenar_pagina(url, sort_root))


def prefijos_documento(base):
    root = raiz_documento(base).lower()
    prefijos = {root}
    simple = re.sub(r'(?i)^circu0*', '', root)
    if simple:
        prefijos.add(simple)
        prefijos.add(simple.lstrip('0') or simple)
    solo_num = re.sub(r'(?i)^circu', '', root)
    if solo_num:
        prefijos.add(solo_num)
        prefijos.add(solo_num.lstrip('0') or solo_num)
    return {p for p in prefijos if p}


def extraer_urls_imagenes(page_url, html_text):
    soup = BeautifulSoup(html_text, 'lxml')
    current_base = os.path.splitext(os.path.basename(urlparse(page_url).path))[0]
    prefijos = prefijos_documento(current_base)
    imagenes = []
    vistos = set()

    for img in soup.find_all('img', src=True):
        src = img.get('src', '').strip()
        if not src:
            continue
        abs_url = urljoin(page_url, src)
        base = os.path.basename(urlparse(abs_url).path)
        stem = os.path.splitext(base)[0].lower()
        if not IMAGE_EXT_RE.search(base):
            continue
        if not any(stem.startswith(prefijo) for prefijo in prefijos):
            continue
        if abs_url in vistos:
            continue
        vistos.add(abs_url)
        imagenes.append(abs_url)

    return imagenes


def es_linea_mayuscula_util(linea):
    letras = [c for c in linea if c.isalpha()]
    if not letras:
        return False
    return (sum(1 for c in letras if c.isupper()) / len(letras)) >= 0.65



def es_linea_mayuscula_util(linea):
    letras = [c for c in linea if c.isalpha()]
    if not letras:
        return False
    return (sum(1 for c in letras if c.isupper()) / len(letras)) >= 0.65


def extraer_encabezado_escaneado(html_text, fallback_title=''):
    soup = BeautifulSoup(html_text, 'lxml')
    titulo_meta, materia_meta = extraer_metadata_html(soup, fallback_title=fallback_title)

    candidatos = []
    for raw in soup.get_text('\n', strip=True).splitlines():
        linea = limpiar_texto(reparar_mojibake(raw))
        if es_linea_ruido_historico(linea):
            continue
        candidatos.append(linea)

    titulo = titulo_meta or ''
    materia = materia_meta or ''
    for idx, linea in enumerate(candidatos):
        upper = linea.upper()
        if not titulo and upper.startswith('CIRCULAR '):
            titulo = normalizar_texto_ocr(linea)
            continue
        if not materia and 'MATERIA:' in upper:
            partes = [linea]
            j = idx + 1
            while j < len(candidatos):
                siguiente = candidatos[j]
                if re.match(r'^[IVXLCDM]+\.-', siguiente):
                    break
                if 'INTRODUCCION' in siguiente.upper():
                    break
                if es_linea_mayuscula_util(siguiente):
                    partes.append(siguiente)
                    j += 1
                    continue
                break
            materia = normalizar_texto_ocr(' '.join(partes))
            break

    if materia.upper().startswith('MATERIA:'):
        materia = limpiar_texto(materia.split(':', 1)[1])

    encabezado = []
    if titulo:
        encabezado.append(titulo)
    if materia:
        encabezado.append(f'MATERIA: {materia}')

    return {
        'titulo_html': titulo or None,
        'materia_html': materia or None,
        'texto': '\n'.join(encabezado).strip(),
    }

def guardar_salidas_ocr(nombre, anio, payload):
    year_dir = ensure_year_dir(OCR_DIR, anio)
    stem = safe_name(f'{anio}_{nombre}')
    raw_path = os.path.join(year_dir, f'{stem}_raw.txt')
    clean_path = os.path.join(year_dir, f'{stem}_clean.txt')
    json_path = os.path.join(year_dir, f'{stem}_ocr.json')

    with open(raw_path, 'w', encoding='utf-8') as f:
        f.write(payload['full_text_raw'])
    with open(clean_path, 'w', encoding='utf-8') as f:
        f.write(payload['full_text_normalized'])
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return (
        os.path.relpath(raw_path, BASE),
        os.path.relpath(clean_path, BASE),
        os.path.relpath(json_path, BASE),
    )
def html_crudo_parece_scan(raw_bytes, url):
    try:
        html_text, _ = decodificar_html(raw_bytes)
    except Exception:
        return False

    current_base = os.path.splitext(os.path.basename(urlparse(url).path))[0]
    prefijos = prefijos_documento(current_base)
    html_lower = html_text.lower()

    if '<img' not in html_lower:
        return False
    if '.gif' not in html_lower and '.jpg' not in html_lower and '.png' not in html_lower and '.tif' not in html_lower:
        return False
    if not any(prefijo in html_lower for prefijo in prefijos) and not re.search(r'\\[\\s*1\\s*\\].*\\[\\s*2\\s*\\]', html_text, re.IGNORECASE | re.DOTALL):
        return False
    return True


def contar_imagenes_documento(soup, page_url):
    current_base = os.path.splitext(os.path.basename(urlparse(page_url).path))[0]
    prefijos = prefijos_documento(current_base)
    total = 0

    for img in soup.find_all('img', src=True):
        src = img.get('src', '').strip()
        if not src:
            continue
        abs_url = urljoin(page_url, src)
        base = os.path.basename(urlparse(abs_url).path)
        if not IMAGE_EXT_RE.search(base):
            continue
        if any(os.path.splitext(base)[0].lower().startswith(prefijo) for prefijo in prefijos):
            total += 1

    return total


def parece_html_escaneado(soup, page_url, texto_limpio):
    imagenes_doc = contar_imagenes_documento(soup, page_url)
    if imagenes_doc == 0:
        return False

    texto_norm = limpiar_texto(texto_limpio)
    palabras = re.findall(r'[A-Za-z0-9]{4,}', texto_norm)

    if len(texto_norm) < 1200:
        return True
    if len(palabras) < 120:
        return True
    return False


def procesar_html_ocr(raw_bytes, url, anio, nombre, fallback_title='', usar_ocr=True):
    html_decodificado, encoding = decodificar_html(raw_bytes)
    html_original, html_limpio = guardar_htmls(nombre, anio, raw_bytes, html_decodificado)
    encabezado = extraer_encabezado_escaneado(html_decodificado, fallback_title=fallback_title)

    paginas = descubrir_paginas_escaneadas(url, html_decodificado)
    imagenes = []
    imagenes_vistas = set()
    hash_md5 = hashlib.md5()
    hash_md5.update(raw_bytes)

    for pagina_url in paginas:
        if pagina_url == url:
            pagina_raw = raw_bytes
            pagina_html = html_decodificado
        else:
            resp = descargar_recurso(pagina_url, timeout=20)
            if not resp:
                continue
            pagina_raw = resp.content
            pagina_html, _ = decodificar_html(pagina_raw)
            pagina_base = os.path.splitext(os.path.basename(urlparse(pagina_url).path))[0]
            guardar_htmls(pagina_base, anio, pagina_raw, pagina_html)
            hash_md5.update(pagina_raw)

        for img_url in extraer_urls_imagenes(pagina_url, pagina_html):
            if img_url in imagenes_vistas:
                continue
            img_resp = descargar_recurso(img_url, timeout=20)
            if not img_resp:
                continue
            img_index = len(imagenes) + 1
            img_path, img_rel = guardar_imagen(nombre, anio, img_index, img_url, img_resp.content)
            imagenes_vistas.add(img_url)
            imagenes.append({'url': img_url, 'path': img_path, 'rel': img_rel})
            hash_md5.update(img_resp.content)

    if not imagenes:
        return {
            'texto': encabezado['texto'] or f'[DOCUMENTO ESCANEADO - SIN IMAGENES DESCARGADAS] Circular {nombre} de {anio}',
            'chars': len(limpiar_texto(encabezado['texto'] or '')),
            'ocr_body': '',
            'ocr_body_chars': 0,
            'ok': False,
            'pendiente_ocr': True,
            'raw_hash': hash_md5.hexdigest(),
            'source_format': 'html_scan',
            'url': url,
            'es_imagen': True,
            'titulo_html': encabezado['titulo_html'],
            'materia_html': encabezado['materia_html'],
            'html_original': html_original,
            'html_limpio': html_limpio,
        }

    pdf_local = construir_pdf_desde_imagenes([img['path'] for img in imagenes], anio, nombre)

    paginas_payload = []
    raw_parts = [encabezado['texto']] if encabezado['texto'] else []
    clean_parts = [encabezado['texto']] if encabezado['texto'] else []
    body_parts = []
    paginas_con_ocr = 0
    backend_global = None
    body_chars = 0

    for idx, info in enumerate(imagenes, start=1):
        texto_raw = None
        engine_usado = None
        if usar_ocr:
            texto_raw = ocr_con_tesseract(info['path'])
            if texto_raw:
                engine_usado = 'tesseract'
                backend_global = backend_global or 'tesseract'
            else:
                texto_raw = ocr_con_rapidocr(info['path'])
                if texto_raw:
                    engine_usado = 'rapidocr'
                    backend_global = backend_global or 'rapidocr'
        if texto_raw:
            paginas_con_ocr += 1
            texto_clean = normalizar_texto_ocr(texto_raw)
            texto_clean = limpiar_texto(texto_clean)
            if texto_clean:
                body_parts.append(f'[PAGINA {idx}]\n{texto_clean}')
            body_chars += len(texto_clean)
            raw_parts.append(f'[PAGINA {idx}]\n{texto_raw.strip()}')
            clean_parts.append(f'[PAGINA {idx}]\n{texto_clean.strip()}')
        else:
            texto_raw = ''
            texto_clean = ''
            raw_parts.append(f'[PAGINA {idx}]\n[OCR NO DISPONIBLE O SIN TEXTO EXTRAIBLE]')
            clean_parts.append(f'[PAGINA {idx}]\n[OCR NO DISPONIBLE O SIN TEXTO EXTRAIBLE]')

        paginas_payload.append({
            'page_index': idx,
            'image_url': info['url'],
            'original_image_path': info['rel'],
            'ocr_engine_used': engine_usado,
            'raw_text': texto_raw,
            'normalized_text': texto_clean,
            'confidence': None,
        })

    full_raw = '\n\n'.join([p for p in raw_parts if p]).strip()
    full_clean = '\n\n'.join([p for p in clean_parts if p]).strip()
    ocr_body = '\n\n'.join(body_parts).strip()
    ocr_payload = {
        'source_url': url,
        'document_id': safe_name(f'{anio}_{nombre}'),
        'fetched_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
        'header': encabezado,
        'pages': paginas_payload,
        'full_text_raw': full_raw,
        'full_text_normalized': full_clean,
        'pdf_local': pdf_local,
        'html_original': html_original,
        'html_limpio': html_limpio,
    }
    ocr_raw_local, ocr_clean_local, ocr_json_local = guardar_salidas_ocr(nombre, anio, ocr_payload)

    ok = body_chars >= 250
    return {
        'texto': full_clean[:50000],
        'chars': len(limpiar_texto(full_clean)),
        'ocr_body': ocr_body[:50000],
        'ocr_body_chars': body_chars,
        'ok': ok,
        'pendiente_ocr': not ok,
        'raw_hash': hash_md5.hexdigest(),
        'pdf_local': pdf_local,
        'source_format': f'html_ocr_{backend_global}' if backend_global else 'html_ocr_pendiente',
        'url': url,
        'es_imagen': True,
        'titulo_html': encabezado['titulo_html'],
        'materia_html': encabezado['materia_html'],
        'html_original': html_original,
        'html_limpio': html_limpio,
        'ocr_raw_local': ocr_raw_local,
        'ocr_clean_local': ocr_clean_local,
        'ocr_json_local': ocr_json_local,
    }

def procesar_html(raw_bytes, response, url, anio, nombre, fallback_title=''):
    html_decodificado, encoding = decodificar_html(raw_bytes)
    soup = BeautifulSoup(html_decodificado, 'lxml')
    titulo_html, materia_html = extraer_metadata_html(soup, fallback_title=fallback_title)
    texto_limpio = extraer_texto_html(soup, titulo=titulo_html, materia=materia_html)
    es_imagen = parece_html_escaneado(soup, url, texto_limpio)

    if es_imagen:
        ocr_resultado = procesar_html_ocr(raw_bytes, url, anio, nombre, fallback_title=fallback_title, usar_ocr=True)
        ocr_resultado['encoding_detectado'] = encoding
        return ocr_resultado

    html_original, html_limpio = guardar_htmls(nombre, anio, raw_bytes, html_decodificado)
    pdf_local = construir_pdf_desde_texto(texto_limpio, anio, nombre, titulo=titulo_html or fallback_title or '')
    return {
        'texto': texto_limpio,
        'encoding_detectado': encoding,
        'es_imagen': False,
        'chars': len(texto_limpio),
        'html_original': html_original,
        'html_limpio': html_limpio,
        'titulo_html': titulo_html or None,
        'materia_html': materia_html or None,
        'ok': len(texto_limpio) > 120,
        'raw_hash': hashlib.md5(raw_bytes).hexdigest(),
        'pdf_local': pdf_local,
        'source_format': 'html',
        'url': url,
    }

def procesar_pdf(pdf_bytes, url, anio, nombre):
    extraido = extraer_texto_pdf(pdf_bytes)
    if not extraido.get('ok') or extraido.get('chars', 0) < 50:
        return None

    year_dir = ensure_year_dir(PDF_HIST_DIR, anio)
    pdf_name = safe_name(f'circular_historica_{anio}_{nombre}.pdf')
    pdf_path = os.path.join(year_dir, pdf_name)
    with open(pdf_path, 'wb') as f:
        f.write(pdf_bytes)

    return {
        'texto': extraido['texto'],
        'chars': extraido['chars'],
        'paginas': extraido['paginas'],
        'ok': True,
        'raw_hash': hashlib.md5(pdf_bytes).hexdigest(),
        'pdf_local': os.path.relpath(pdf_path, BASE),
        'source_format': 'pdf',
        'url': url,
        'es_imagen': False,
    }


def procesar_doc(doc_bytes, url, anio, nombre):
    doc_path, doc_rel = guardar_doc(nombre, anio, doc_bytes)
    intentos = [
        ('doc_antiword', extraer_doc_con_antiword),
        ('doc_soffice', extraer_doc_con_soffice),
        ('doc_word', extraer_doc_con_word),
    ]

    for metodo, funcion in intentos:
        texto = funcion(doc_path)
        if not texto:
            continue
        texto = limpiar_texto(texto)
        if len(texto) < 50:
            continue
        return {
            'texto': texto[:50000],
            'chars': len(texto),
            'ok': True,
            'raw_hash': hashlib.md5(doc_bytes).hexdigest(),
            'doc_local': doc_rel,
            'source_format': metodo,
            'url': url,
            'es_imagen': False,
        }

    return {
        'ok': False,
        'pendiente_doc': True,
        'source_format': 'doc',
        'url': url,
        'doc_local': doc_rel,
    }


def descargar_circular_historica(base_url, nombre_archivo, anio, fallback_title=''):
    base = nombre_archivo.rsplit('.', 1)[0]
    urls_intentar = [
        f'{base_url}/{base}.pdf',
        f'{base_url}/{base}.htm',
        f'{base_url}/{base}.html',
        f'{base_url}/{base}.doc',
    ]

    visto = set()
    for url in urls_intentar:
        if url in visto:
            continue
        visto.add(url)
        try:
            response = SESSION.get(url, timeout=15)
        except Exception as e:
            log.warning(f'[historico] error descarga {url}: {e}')
            continue

        if response.status_code != 200 or len(response.content) <= 500:
            continue

        content_type = response.headers.get('Content-Type', '').lower()
        if 'pdf' in content_type or url.lower().endswith('.pdf'):
            return procesar_pdf(response.content, url, anio, base)
        if url.lower().endswith('.doc') or 'msword' in content_type or 'application/octet-stream' in content_type:
            return procesar_doc(response.content, url, anio, base)
        if html_crudo_parece_scan(response.content, url):
            return procesar_html_ocr(response.content, url, anio, base, fallback_title=fallback_title, usar_ocr=True)
        return procesar_html(response.content, response, url, anio, base, fallback_title=fallback_title)

    return None


def construir_doc_data(item, resultado):
    numero = str(item['numero']).strip()
    anio = int(item['anio'])
    texto = resultado['texto']
    titulo = limpiar_texto(normalizar_texto_ocr(resultado.get('titulo_html') or item.get('titulo') or ''))
    if not titulo:
        titulo = f'Circular N {numero} de {anio}'

    materia = limpiar_texto(normalizar_texto_ocr(resultado.get('materia_html') or ''))
    if materia.upper().startswith('MATERIA:'):
        materia = limpiar_texto(materia.split(':', 1)[1])

    bloques = [titulo]
    if materia:
        bloques.append(f'MATERIA: {materia}')

    if resultado.get('es_imagen'):
        ocr_body = re.sub(r'[ \t]+', ' ', (resultado.get('ocr_body') or '')).strip()
        bloques.append('[DOCUMENTO ESCANEADO DEL SII]')
        if ocr_body:
            bloques.append('[OCR AUXILIAR NO REVISADO]')
            bloques.append(ocr_body)
            contenido_indexable = '\n\n'.join(b for b in bloques if b)
        else:
            bloques.append('Se conserva el encabezado limpio y el PDF escaneado. El OCR queda como apoyo auxiliar y no se indexa como contenido principal.')
            contenido_indexable = '\n\n'.join(b for b in bloques if b)
        fecha = extraer_fecha_texto(contenido_indexable) or f'{anio}-01-01'
        leyes = detectar_leyes(ocr_body) if ocr_body else []
        articulos = detectar_articulos(ocr_body) if ocr_body else []
    else:
        cuerpo = re.sub(r'[ \t]+', ' ', (texto or '')).strip()
        if cuerpo:
            bloques.append(cuerpo)
        contenido_indexable = '\n\n'.join(b for b in bloques if b)
        fecha = extraer_fecha_texto(contenido_indexable) or f'{anio}-01-01'
        leyes = detectar_leyes(cuerpo)
        articulos = detectar_articulos(cuerpo)

    resumen = extraer_resumen(contenido_indexable)

    palabras = []
    if resultado.get('es_imagen'):
        palabras.append('documento_escaneado')
        palabras.append('ocr_auxiliar')
    if resultado.get('source_format', '').startswith('html_ocr_'):
        palabras.append('ocr_extraido')
        palabras.append(resultado.get('source_format'))
    palabras_clave = ','.join(palabras) if palabras else None

    return {
        'hash_md5': resultado['raw_hash'],
        'tipo': 'circular',
        'numero': numero,
        'anio': anio,
        'fecha': fecha,
        'titulo': titulo[:500],
        'materia': materia[:500] if materia else None,
        'subtema': (materia or item.get('descripcion') or '')[:300],
        'contenido': contenido_indexable,
        'resumen': resumen,
        'url_sii': resultado['url'],
        'referencia': f'Circular N{numero} de {anio}',
        'palabras_clave': palabras_clave,
        'leyes_citadas': json.dumps(leyes),
        'articulos_clave': json.dumps(articulos[:20]),
        'fuente': 'scraper_historico',
    }

def procesar_item(item, delay=0.8):
    anio = item['anio']
    numero = str(item['numero']).strip()
    tipo_log = 'circular_historica'

    if doc_existe('circular', numero, anio):
        return {'estado': 'existente', 'numero': numero}

    base_url = f'{BASE_URL}/{anio}'
    resultado = descargar_circular_historica(base_url, item['nombre_archivo'], anio, fallback_title=item.get('titulo') or '')
    if not resultado:
        log_scraper(tipo_log, anio, numero, 'no_encontrado', item.get('href_original') or base_url)
        return {'estado': 'no_encontrado', 'numero': numero}

    if resultado.get('pendiente_doc'):
        log_scraper(tipo_log, anio, numero, 'doc_pendiente', resultado['url'])
        return {'estado': 'doc_pendiente', 'numero': numero}

    if resultado.get('pendiente_ocr') and not resultado.get('ok') and not resultado.get('es_imagen'):
        log_scraper(tipo_log, anio, numero, 'ocr_pendiente', resultado['url'])
        return {'estado': 'ocr_pendiente', 'numero': numero}

    if not resultado.get('ok') and not resultado.get('pendiente_ocr'):
        log_scraper(tipo_log, anio, numero, 'procesamiento_fallido', resultado['url'])
        return {'estado': 'procesamiento_fallido', 'numero': numero}

    if doc_existe_hash(resultado['raw_hash']):
        return {'estado': 'hash_existente', 'numero': numero}

    doc_data = construir_doc_data(item, resultado)
    doc_id = guardar_documento(doc_data)
    if not doc_id:
        log_scraper(tipo_log, anio, numero, 'error_bd', resultado['url'])
        return {'estado': 'error_bd', 'numero': numero}

    if resultado.get('es_imagen'):
        if resultado.get('pendiente_ocr'):
            estado = 'ok_scan_metadata_pendiente_ocr'
        elif resultado.get('ocr_body_chars', 0) >= 250:
            estado = 'ok_scan_ocr_auxiliar'
        else:
            estado = 'ok_scan_metadata'
    elif resultado.get('pendiente_ocr'):
        estado = 'ok_html_ocr_pendiente'
    else:
        estado = f"ok_{resultado.get('source_format', 'desconocido')}"

    log_scraper(tipo_log, anio, numero, estado, resultado['url'])
    time.sleep(delay)
    return {
        'estado': estado,
        'numero': numero,
        'doc_id': doc_id,
        'chars': resultado.get('chars', 0),
        'url': resultado['url'],
    }


def procesar_anio(anio, solo_indice=False, delay=0.8):
    index_url, html, encoding = descargar_indice(anio)
    if not index_url:
        print(f'[WARN] {anio}: indice no encontrado')
        return {'anio': anio, 'indice': None, 'total': 0, 'nuevos': 0, 'errores': 1}

    items = descubrir_circulares(anio, index_url, html)
    print(f'[{anio}] indice={index_url} encoding={encoding} circulares={len(items)}')

    if solo_indice:
        for item in items:
            print(f"  - {item['numero']}: {item['titulo']} [{','.join(item['extensiones'])}]")
        return {'anio': anio, 'indice': index_url, 'total': len(items), 'nuevos': 0, 'errores': 0}

    nuevos = errores = 0
    for idx, item in enumerate(items, start=1):
        resultado = procesar_item(item, delay=delay)
        estado = resultado['estado']
        if estado.startswith('ok_'):
            nuevos += 1
            print(f"  [ok] [{idx}/{len(items)}] Circ {item['numero']}/{anio} -> {estado}")
        elif estado in {'existente', 'hash_existente'}:
            print(f"  [skip] [{idx}/{len(items)}] Circ {item['numero']}/{anio} -> {estado}")
        else:
            errores += 1
            print(f"  [warn] [{idx}/{len(items)}] Circ {item['numero']}/{anio} -> {estado}")

    return {'anio': anio, 'indice': index_url, 'total': len(items), 'nuevos': nuevos, 'errores': errores}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Descargar circulares historicas SII')
    parser.add_argument('--anio', type=int, help='Descargar solo un ano')
    parser.add_argument('--desde', type=int, default=1990, help='Ano desde')
    parser.add_argument('--hasta', type=int, default=2012, help='Ano hasta')
    parser.add_argument('--delay', type=float, default=0.8, help='Delay entre descargas')
    parser.add_argument('--solo-indice', action='store_true', help='Solo mostrar que hay, sin descargar')
    args = parser.parse_args()

    if args.anio:
        anios = [args.anio]
    else:
        anios = list(range(args.hasta, args.desde - 1, -1))

    resumenes = []
    for anio in anios:
        resumenes.append(procesar_anio(anio, solo_indice=args.solo_indice, delay=args.delay))

    total = sum(r['total'] for r in resumenes)
    nuevos = sum(r['nuevos'] for r in resumenes)
    errores = sum(r['errores'] for r in resumenes)
    print('-' * 72)
    print(f'TOTAL indices/documentos vistos: {total}')
    print(f'NUEVOS: {nuevos}')
    print(f'ERRORES/WARN: {errores}')

