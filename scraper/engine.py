"""
SII Normativa — Motor de Scraping v2.1
=======================================
ARQUITECTURA CORRECTA (verificada con HTML fuente del SII):

Circulares / Resoluciones:
  → Índice HTML + PDF directo por URL

Oficios (IVA, LIR, Otras Normas):
  → API JSON: POST https://www3.sii.cl/getPublicacionesCTByMateria
     Body: {"key":"IVA","year":"2026"}
     Campos: pubNumOficio, pubFechaPubli, pubLegal, pubResumen,
             idBlobArchPublica, extensionArchPublica, mTypeArchPublica
  → PDF: POST https://www4.sii.cl/gabineteAdmInternet/descargaArchivo
     Campos: nombreDocumento, extension, acc, id, mediaType
"""

import os, sys, time, json, hashlib, re, logging, textwrap
import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
from urllib.parse import urljoin
import fitz  # PyMuPDF
from pdf_layout import build_pdf_path

BASE    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE, 'data', 'sii_normativa.db')
PDF_DIR = os.path.join(BASE, 'pdfs')
LOG_DIR = os.path.join(BASE, 'logs')
os.makedirs(PDF_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f'scraper_{date.today().isoformat()}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger('sii_scraper')

SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'es-CL,es;q=0.9,en;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
})

SII_BASE     = "https://www.sii.cl/normativa_legislacion"
SII_API      = "https://www3.sii.cl"
DESCARGA_URL = "https://www4.sii.cl/gabineteAdmInternet/descargaArchivo"

INDICES = {
    'circular':   f"{SII_BASE}/circulares/{{anio}}/indcir{{anio}}.htm",
    'resolucion': f"{SII_BASE}/resoluciones/{{anio}}/indres{{anio}}.htm",
}
PDF_URLS = {
    'circular':   f"{SII_BASE}/circulares/{{anio}}/circu{{num}}.pdf",
    'resolucion': f"{SII_BASE}/resoluciones/{{anio}}/reso{{num}}.pdf",
}

LEGACY_CIRCULARES_BASE = "https://www.sii.cl/documentos/circulares"

def obtener_urls_indice(tipo_clave, anio):
    if tipo_clave == 'circular' and anio <= 2012:
        return [
            f"{LEGACY_CIRCULARES_BASE}/{anio}/indcir{anio}.htm",
            INDICES[tipo_clave].format(anio=anio),
        ]
    return [INDICES[tipo_clave].format(anio=anio)]

def obtener_urls_pdf(tipo_clave, anio, numero):
    if tipo_clave == 'circular' and anio <= 2012:
        return [
            f"{LEGACY_CIRCULARES_BASE}/{anio}/circu{numero}.pdf",
            PDF_URLS[tipo_clave].format(anio=anio, num=numero),
        ]
    return [PDF_URLS[tipo_clave].format(anio=anio, num=numero)]


# Claves verificadas en el HTML fuente del portal SII
OFICIO_API_KEYS = {
    'oficio_iva':   'IVA',    # Ley Impuesto Ventas y Servicios
    'oficio_lir':   'RENTA',  # Ley Impuesto a la Renta
    'oficio_otras': 'OTROS',  # Otras Normas (CT, Timbres, Herencias...)
}


def oficio_pdf_categoria(tipo_clave, doc_info=None):
    doc_info = doc_info or {}
    api_key = (doc_info.get('_api_key') or '').strip().upper()
    if tipo_clave == 'oficio_lir' or api_key == 'RENTA':
        return 'lir'
    if tipo_clave == 'oficio_iva' or api_key == 'IVA':
        return 'iva'
    if tipo_clave == 'oficio_otras' or api_key in {'OTRAS', 'OTROS'}:
        return 'otras_normas'

    subtema = (doc_info.get('pubLegal') or doc_info.get('descripcion') or '').upper()
    if 'VENTAS' in subtema or 'IVA' in subtema:
        return 'iva'
    if 'RENTA' in subtema or 'LIR' in subtema:
        return 'lir'
    return 'otras_normas'


def oficio_subtema_prefijo(tipo_clave, doc_info=None):
    doc_info = doc_info or {}
    api_key = (doc_info.get('_api_key') or '').strip().upper()
    if tipo_clave == 'oficio_lir' or api_key == 'RENTA':
        return 'RENTA'
    if tipo_clave == 'oficio_iva' or api_key == 'IVA':
        return 'IVA'
    if tipo_clave == 'oficio_otras' or api_key in {'OTRAS', 'OTROS'}:
        return 'OTRAS'
    return 'OTRAS'

MESES = {
    'enero': '01',
    'febrero': '02',
    'marzo': '03',
    'abril': '04',
    'mayo': '05',
    'junio': '06',
    'julio': '07',
    'agosto': '08',
    'septiembre': '09',
    'setiembre': '09',
    'octubre': '10',
    'noviembre': '11',
    'diciembre': '12',
}


# ── DB ────────────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def doc_existe_hash(hash_md5):
    conn = get_db()
    r = conn.execute("SELECT id FROM documentos WHERE hash_md5=?", (hash_md5,)).fetchone()
    conn.close()
    return r is not None

def doc_id_por_hash(hash_md5):
    conn = get_db()
    r = conn.execute("SELECT id FROM documentos WHERE hash_md5=?", (hash_md5,)).fetchone()
    conn.close()
    return r['id'] if r else None

def doc_existe_url(url_sii):
    conn = get_db()
    r = conn.execute("SELECT id FROM documentos WHERE url_sii=?", (url_sii,)).fetchone()
    conn.close()
    return r is not None

def doc_id_por_url(url_sii):
    conn = get_db()
    r = conn.execute("SELECT id FROM documentos WHERE url_sii=?", (url_sii,)).fetchone()
    conn.close()
    return r['id'] if r else None

def doc_existe(tipo, numero, anio):
    tipo_n = tipo.split('_')[0]
    conn = get_db()
    r = conn.execute("SELECT id FROM documentos WHERE tipo=? AND numero=? AND anio=?",
                     (tipo_n, numero, anio)).fetchone()
    conn.close()
    return r is not None

def ensure_oficio_fuentes_table():
    conn = get_db()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS oficio_fuentes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id INTEGER NOT NULL REFERENCES documentos(id) ON DELETE CASCADE,
                anio INTEGER,
                api_key TEXT,
                numero TEXT,
                blob_id TEXT UNIQUE,
                url_sii TEXT,
                fecha_pub TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_oficio_fuentes_doc ON oficio_fuentes(doc_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_oficio_fuentes_anio ON oficio_fuentes(anio)")
        conn.commit()
    finally:
        conn.close()

def registrar_oficio_fuente(doc_id, anio, api_key, numero, blob_id, url_sii, fecha_pub):
    if not doc_id or not blob_id:
        return
    conn = get_db()
    try:
        conn.execute("""
            INSERT INTO oficio_fuentes(doc_id, anio, api_key, numero, blob_id, url_sii, fecha_pub)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(blob_id) DO UPDATE SET
                doc_id=excluded.doc_id,
                anio=excluded.anio,
                api_key=excluded.api_key,
                numero=excluded.numero,
                url_sii=excluded.url_sii,
                fecha_pub=excluded.fecha_pub
        """, (doc_id, anio, api_key, numero, blob_id, url_sii, fecha_pub))
        conn.commit()
    finally:
        conn.close()

# ── PyMuPDF ───────────────────────────────────────────────────────────────
def extraer_texto_pdf(pdf_bytes):
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        texto = "\n".join(p.get_text("text") for p in doc)
        texto = re.sub(r'\n{3,}', '\n\n', texto)
        texto = re.sub(r'[ \t]+', ' ', texto).strip()
        return {'texto': texto, 'paginas': len(doc), 'chars': len(texto), 'ok': True}
    except Exception as e:
        return {'texto': '', 'paginas': 0, 'chars': 0, 'ok': False, 'error': str(e)}

def _soup_a_texto_legible(soup):
    soup = BeautifulSoup(str(soup), 'html.parser')

    for tag in soup(['script', 'style', 'noscript']):
        tag.decompose()

    for br in soup.find_all('br'):
        br.replace_with('\n')

    for table in soup.find_all('table'):
        filas = []
        for tr in table.find_all('tr'):
            celdas = [
                re.sub(r'\s+', ' ', td.get_text(' ', strip=True)).strip()
                for td in tr.find_all(['th', 'td'])
            ]
            celdas = [c for c in celdas if c]
            if celdas:
                filas.append(' | '.join(celdas))
        bloque = '\n'.join(filas).strip()
        table.replace_with(f"\n{bloque}\n" if bloque else '\n')

    texto = soup.get_text('\n', strip=True)
    texto = re.sub(r'\n{3,}', '\n\n', texto)

    inicio = re.search(r'circular\s*n', texto, re.IGNORECASE)
    if inicio:
        texto = texto[inicio.start():]

    return texto.strip()

def _generar_pdf_desde_texto(titulo, texto):
    contenido = (f"{titulo}\n\n{texto}" if titulo else texto).strip()
    if not contenido:
        return None

    doc = fitz.open()
    rect = fitz.paper_rect('a4')
    margen_x = 42
    margen_y = 48
    line_height = 14
    max_width = 95

    lineas = []
    for bloque in contenido.splitlines():
        bloque = bloque.rstrip()
        if not bloque:
            lineas.append('')
            continue
        partes = textwrap.wrap(
            bloque,
            width=max_width,
            replace_whitespace=False,
            drop_whitespace=False,
        ) or ['']
        lineas.extend([p.rstrip() for p in partes])

    pagina = None
    y = rect.height
    for linea in lineas:
        if pagina is None or y + line_height > rect.height - margen_y:
            pagina = doc.new_page(width=rect.width, height=rect.height)
            y = margen_y
        pagina.insert_text((margen_x, y), linea, fontsize=10.5, fontname='helv')
        y += line_height

    return doc.tobytes()

def _descargar_circular_html(url_detalle, anio, doc_info):
    try:
        r = SESSION.get(url_detalle, timeout=20)
        if r.status_code != 200:
            return None
        r.encoding = 'utf-8'
        soup = BeautifulSoup(r.text, 'html.parser')
        texto = _soup_a_texto_legible(soup)
        if len(texto) < 80:
            return None

        titulo = (doc_info.get('titulo') or '').strip()
        if not titulo:
            primera = next((linea.strip() for linea in texto.splitlines() if linea.strip()), '')
            titulo = primera[:500]

        fecha = _extraer_fecha_circular(texto, anio)
        descripcion = doc_info.get('descripcion') or extraer_resumen(texto, max_chars=300)
        pdf_bytes = _generar_pdf_desde_texto(titulo, texto)
        if not pdf_bytes:
            return None

        return {
            'pdf_bytes': pdf_bytes,
            'titulo': titulo,
            'descripcion': descripcion,
            'fecha': fecha,
        }
    except Exception as e:
        log.warning(f"  No se pudo convertir HTML a PDF para {url_detalle}: {e}")
        return None

# ── Análisis ──────────────────────────────────────────────────────────────
LEYES_MAP = {
    'LIR':  [r'impuesto\s+a\s+la\s+renta', r'd\.?l\.?\s*824'],
    'LIVS': [r'impuesto\s+a\s+las\s+ventas', r'd\.?l\.?\s*825'],
    'CT':   [r'código\s+tributario', r'd\.?l\.?\s*830'],
    'LTE':  [r'ley\s+de\s+timbres', r'decreto\s+ley\s+3\.?475'],
    'LH':   [r'ley\s+16\.?271', r'ley\s+de\s+herencias'],
    'LMT':  [r'ley\s+n[°o]?\s*21\.?210'],
}

def detectar_leyes(texto):
    t = texto.lower()
    return list({ley for ley, pats in LEYES_MAP.items() if any(re.search(p, t) for p in pats)})

def detectar_articulos(texto):
    arts = []
    for p in [r'artículos?\s+(\d+[\s°]*(?:bis|ter)?)', r'art\.\s+(\d+[\s°]*(?:bis|ter)?)']:
        for m in re.finditer(p, texto, re.IGNORECASE):
            a = re.sub(r'\s+', ' ', m.group(1).strip())
            if len(a) < 30: arts.append(f"art. {a}")
    seen = set(); result = []
    for a in arts:
        if a not in seen: seen.add(a); result.append(a)
    return result[:30]

def extraer_fecha_texto(texto):
    MESES = {'enero':'01','febrero':'02','marzo':'03','abril':'04','mayo':'05',
              'junio':'06','julio':'07','agosto':'08','septiembre':'09',
              'octubre':'10','noviembre':'11','diciembre':'12'}
    m = re.search(
        r'(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})',
        texto[:2000], re.IGNORECASE)
    if m:
        d, mes, y = m.groups()
        return f"{y}-{MESES[mes.lower()]}-{d.zfill(2)}"
    m2 = re.search(r'(\d{4})-(\d{2})-(\d{2})', texto[:1000])
    return m2.group(0) if m2 else None


def extraer_fecha_oficio(texto, numero=None):
    encabezado = (texto or '')[:3000]
    patrones = []
    if numero:
        nro = re.escape(str(numero).strip())
        patrones.extend([
            rf'ORD\.\s*N[°ºo]?\s*{nro}\s*,?\s*DE\s*(\d{{1,2}}[./-]\d{{1,2}}[./-]\d{{4}})',
            rf'OFICIO\s+ORDINARIO\s*N[°ºo]?\s*{nro}\s*,?\s*DE\s*(\d{{1,2}}[./-]\d{{1,2}}[./-]\d{{4}})',
            rf'N[°ºo]?\s*{nro}\s*,?\s*DE\s*(\d{{1,2}}[./-]\d{{1,2}}[./-]\d{{4}})',
        ])
    patrones.append(r'ORD\.\s*N[°ºo]?\s*\d+\s*,?\s*DE\s*(\d{1,2}[./-]\d{1,2}[./-]\d{4})')

    for patron in patrones:
        m = re.search(patron, encabezado, re.IGNORECASE)
        if not m:
            continue
        fecha_bruta = m.group(1).replace('/', '.').replace('-', '.')
        m_fecha = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', fecha_bruta)
        if m_fecha:
            d, mo, y = m_fecha.groups()
            return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"
    return None

def _convertir_fecha(fecha_str, anio):
    if not fecha_str: return f"{anio}-01-01"
    m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', fecha_str)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"
    if re.match(r'\d{4}-\d{2}-\d{2}', fecha_str): return fecha_str
    return f"{anio}-01-01"

def extraer_resumen(texto, max_chars=800):
    lineas = [l.strip() for l in texto.split('\n') if len(l.strip()) > 40]
    r = ' '.join(lineas[:5])
    return r[:max_chars]

# ── BD guardar ────────────────────────────────────────────────────────────
def _json_list(value):
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def _indexar_articulos_conservador(conn, doc_id, leyes, articulos):
    leyes = [ley for ley in leyes if ley]
    articulos = [art for art in articulos if art][:20]
    conn.execute("DELETE FROM articulos_idx WHERE doc_id=?", (doc_id,))
    vistos = set()

    if len(leyes) == 1:
        ley = leyes[0]
        if articulos:
            for articulo in articulos:
                key = (ley, articulo)
                if key in vistos:
                    continue
                conn.execute(
                    "INSERT OR IGNORE INTO articulos_idx(doc_id, ley, articulo) VALUES(?,?,?)",
                    (doc_id, ley, articulo),
                )
                vistos.add(key)
        else:
            conn.execute(
                "INSERT OR IGNORE INTO articulos_idx(doc_id, ley, articulo) VALUES(?,?,NULL)",
                (doc_id, ley),
            )
        return

    for ley in leyes:
        key = (ley, None)
        if key in vistos:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO articulos_idx(doc_id, ley, articulo) VALUES(?,?,NULL)",
            (doc_id, ley),
        )
        vistos.add(key)

    for articulo in articulos:
        key = (None, articulo)
        if key in vistos:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO articulos_idx(doc_id, ley, articulo) VALUES(?,NULL,?)",
            (doc_id, articulo),
        )
        vistos.add(key)

def guardar_documento(data):
    payload = dict(data)
    payload.setdefault('paginas', 0)
    payload.setdefault('chars_texto', len(payload.get('contenido') or ''))
    payload.setdefault('pdf_local', None)
    payload.setdefault('pdf_size_bytes', 0)
    payload.setdefault('fuente', 'scraper')

    contenido = (payload.get('contenido') or '').strip()
    if not contenido:
        log.warning('Documento sin contenido util: %s/%s/%s', payload.get('tipo'), payload.get('anio'), payload.get('numero'))
        return None

    if not payload.get('hash_md5'):
        log.warning('Documento sin hash: %s/%s/%s', payload.get('tipo'), payload.get('anio'), payload.get('numero'))
        return None

    pdf_local = (payload.get('pdf_local') or '').strip()
    if not pdf_local:
        log.warning('Documento sin pdf_local: %s/%s/%s', payload.get('tipo'), payload.get('anio'), payload.get('numero'))
        return None

    abs_pdf_path = pdf_local if os.path.isabs(pdf_local) else os.path.join(BASE, pdf_local.replace('/', os.sep))
    if not os.path.exists(abs_pdf_path):
        log.warning('Documento con pdf_local inexistente: %s', abs_pdf_path)
        return None

    if int(payload.get('pdf_size_bytes') or 0) <= 0:
        try:
            payload['pdf_size_bytes'] = os.path.getsize(abs_pdf_path)
        except OSError:
            log.warning('Documento sin pdf_size_bytes util: %s/%s/%s', payload.get('tipo'), payload.get('anio'), payload.get('numero'))
            return None

    if int(payload.get('paginas') or 0) <= 0:
        log.warning('Documento con paginas no pobladas: %s/%s/%s', payload.get('tipo'), payload.get('anio'), payload.get('numero'))
        return None

    if int(payload.get('chars_texto') or 0) <= 0:
        payload['chars_texto'] = len(contenido)
        if payload['chars_texto'] <= 0:
            log.warning('Documento con chars_texto no util: %s/%s/%s', payload.get('tipo'), payload.get('anio'), payload.get('numero'))
            return None

    conn = get_db()
    try:
        same_identity = conn.execute(
            "SELECT id, hash_md5 FROM documentos WHERE tipo=? AND numero=? AND anio=?",
            (payload.get('tipo'), payload.get('numero'), payload.get('anio')),
        ).fetchone()
        enforce_identity = payload.get('tipo') not in {'oficio', 'judicial'}
        if enforce_identity and same_identity and same_identity['hash_md5'] != payload['hash_md5']:
            log.warning(
                "Identidad documental existente con hash distinto; se actualiza: %s/%s/%s",
                payload.get('tipo'), payload.get('anio'), payload.get('numero')
            )
            payload['id'] = same_identity['id']
            conn.execute('''UPDATE documentos SET
                hash_md5=:hash_md5, fecha=:fecha, titulo=:titulo, materia=:materia, subtema=:subtema,
                contenido=:contenido, resumen=:resumen, url_sii=:url_sii, referencia=:referencia,
                palabras_clave=:palabras_clave, leyes_citadas=:leyes_citadas, articulos_clave=:articulos_clave,
                paginas=:paginas, chars_texto=:chars_texto, pdf_local=:pdf_local, pdf_size_bytes=:pdf_size_bytes,
                fuente=:fuente, fecha_carga=datetime('now')
                WHERE id=:id''', payload)
            doc_id = same_identity['id']
            _indexar_articulos_conservador(
                conn,
                doc_id,
                _json_list(payload.get('leyes_citadas')),
                _json_list(payload.get('articulos_clave')),
            )
            conn.commit()
            return doc_id

        conn.execute('''INSERT OR IGNORE INTO documentos
            (hash_md5, tipo, numero, anio, fecha, titulo, materia, subtema,
             contenido, resumen, url_sii, referencia, palabras_clave,
             leyes_citadas, articulos_clave, paginas, chars_texto,
             pdf_local, pdf_size_bytes, fuente)
            VALUES
            (:hash_md5, :tipo, :numero, :anio, :fecha, :titulo, :materia, :subtema,
             :contenido, :resumen, :url_sii, :referencia, :palabras_clave,
             :leyes_citadas, :articulos_clave, :paginas, :chars_texto,
             :pdf_local, :pdf_size_bytes, :fuente)''', payload)
        row = conn.execute("SELECT id FROM documentos WHERE hash_md5=?", (payload['hash_md5'],)).fetchone()
        doc_id = row['id'] if row else None
        if doc_id:
            _indexar_articulos_conservador(
                conn,
                doc_id,
                _json_list(payload.get('leyes_citadas')),
                _json_list(payload.get('articulos_clave')),
            )
        conn.commit()
        return doc_id
    except Exception as e:
        log.error(f"Error BD: {e}")
        return None
    finally:
        conn.close()


def log_scraper(tipo, anio, numero, estado, url):
    conn = get_db()
    try:
        conn.execute("INSERT INTO scraper_log(tipo,anio,numero,estado,url) VALUES(?,?,?,?,?)",(tipo,anio,numero,estado,url))
        conn.commit()
    except: pass
    finally: conn.close()

# ── Parseo índices HTML ───────────────────────────────────────────────────
def _limpiar_texto_html(texto):
    return re.sub(r'\s+', ' ', (texto or '')).strip()

def _extraer_numero_circular(titulo, href=''):
    fuentes = [titulo or '', href or '']
    patrones = [
        r'circular\s*n(?:\D{0,2})\s*0*(\d+)',
        r'circu(?:lar)?0*(\d+)\.(?:pdf|htm|html)',
    ]
    for fuente in fuentes:
        for patron in patrones:
            m = re.search(patron, fuente, re.IGNORECASE)
            if m:
                return m.group(1)
    return None

def _extraer_fecha_circular(texto, anio):
    texto = _limpiar_texto_html(texto)
    m = re.search(
        r'(\d{1,2})\s+de\s+([a-zA-Z]+)\s+del?\s+(\d{4})',
        texto,
        re.IGNORECASE
    )
    if not m:
        return f"{anio}-01-01"
    dia, mes, year = m.groups()
    mes_num = MESES.get(mes.lower())
    if not mes_num:
        return f"{anio}-01-01"
    return f"{year}-{mes_num}-{dia.zfill(2)}"

def _extraer_descripcion_anchor(anchor, titulo):
    partes = []
    for sibling in anchor.next_siblings:
        if getattr(sibling, 'name', None) == 'a':
            break
        texto = sibling.get_text(' ', strip=True) if hasattr(sibling, 'get_text') else str(sibling)
        texto = _limpiar_texto_html(texto)
        if texto:
            partes.append(texto)
        if len(' '.join(partes)) >= 600:
            break

    if partes:
        return ' '.join(partes)[:1000]

    parent = anchor.find_parent(['p', 'li', 'td', 'div'])
    if not parent:
        return ''

    contexto = _limpiar_texto_html(parent.get_text(' ', strip=True))
    if titulo and contexto.startswith(titulo):
        contexto = _limpiar_texto_html(contexto[len(titulo):])
    return contexto[:1000]

def _resolver_url_pdf_desde_detalle(url_detalle):
    try:
        r = SESSION.get(url_detalle, timeout=15)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, 'html.parser')
        for tag in soup.find_all(['a', 'iframe', 'embed', 'object']):
            href = tag.get('href') or tag.get('src') or tag.get('data')
            if href and re.search(r'\.pdf(?:$|\?)', href, re.IGNORECASE):
                return urljoin(url_detalle, href)
        m = re.search(r"[\"']([^\"']+\.pdf(?:\?[^\"']*)?)[\"']", r.text, re.IGNORECASE)
        if m:
            return urljoin(url_detalle, m.group(1))
    except Exception as e:
        log.warning(f"  No se pudo resolver PDF desde detalle {url_detalle}: {e}")
    return None

def _merge_doc_por_numero(docs):
    merged = {}
    for doc in docs:
        numero = str(doc.get('numero', '')).strip()
        if not numero:
            continue
        if numero not in merged:
            merged[numero] = dict(doc)
            continue
        base = merged[numero]
        for key in ('titulo', 'descripcion', 'fecha', 'url_pdf', 'url_detalle'):
            nuevo = doc.get(key)
            if not nuevo:
                continue
            if key == 'descripcion':
                if len(nuevo) > len(base.get(key, '')):
                    base[key] = nuevo
            elif not base.get(key):
                base[key] = nuevo
    return list(merged.values())

def _parsear_indice_circulares_moderno(html, anio, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    docs = []
    for h5 in soup.find_all('h5'):
        a = h5.find('a')
        if not a:
            continue
        href = a.get('href', '')
        numero = _extraer_numero_circular(a.get_text(' ', strip=True), href)
        if not numero:
            continue
        fecha = _extraer_fecha_circular(h5.get_text(' ', strip=True), anio)
        sig = h5.find_next_sibling()
        desc = sig.get_text(strip=True) if sig and sig.name == 'p' else ''
        doc = {
            'numero': numero,
            'titulo': _limpiar_texto_html(a.get_text(' ', strip=True)),
            'descripcion': _limpiar_texto_html(desc),
            'fecha': fecha,
        }
        if href:
            abs_url = urljoin(base_url, href)
            if re.search(r'\.pdf(?:$|\?)', href, re.IGNORECASE):
                doc['url_pdf'] = abs_url
            else:
                doc['url_detalle'] = abs_url
        docs.append(doc)
    return docs

def _parsear_indice_circulares_legacy(html, anio, base_url):
    soup = BeautifulSoup(html, 'html.parser')
    docs = []
    for a in soup.find_all('a', href=True):
        titulo = _limpiar_texto_html(a.get_text(' ', strip=True))
        if not re.search(r'circular\s*n(?:\D{0,2})\s*\d+', titulo, re.IGNORECASE):
            continue
        href = a.get('href', '').strip()
        numero = _extraer_numero_circular(titulo, href)
        if not numero:
            continue
        doc = {
            'numero': numero,
            'titulo': titulo,
            'descripcion': _extraer_descripcion_anchor(a, titulo),
            'fecha': _extraer_fecha_circular(titulo, anio),
        }
        abs_url = urljoin(base_url, href)
        if re.search(r'\.pdf(?:$|\?)', href, re.IGNORECASE):
            doc['url_pdf'] = abs_url
        else:
            doc['url_detalle'] = abs_url
        docs.append(doc)
    return docs

def parsear_indice_circulares(html, anio, base_url):
    if anio <= 2012:
        docs = _parsear_indice_circulares_legacy(html, anio, base_url)
        if not docs:
            docs = _parsear_indice_circulares_moderno(html, anio, base_url)
    else:
        docs = _parsear_indice_circulares_moderno(html, anio, base_url)
        if not docs:
            docs = _parsear_indice_circulares_legacy(html, anio, base_url)
    return _merge_doc_por_numero(docs)

def parsear_indice_resoluciones(html, anio):
    soup = BeautifulSoup(html, 'html.parser')
    docs = []
    for a in soup.find_all('a', href=True):
        m = re.search(r'reso(\d+)\.pdf', a.get('href',''), re.IGNORECASE)
        if not m: continue
        parent = a.find_parent(['li','p','td','h5'])
        desc = parent.get_text(strip=True) if parent else a.get_text(strip=True)
        docs.append({'numero': m.group(1), 'titulo': a.get_text(strip=True), 'descripcion': desc, 'fecha': f"{anio}-01-01"})
    return docs

# ── API Oficios SII ───────────────────────────────────────────────────────
def obtener_oficios_api(api_key, anio):
    """
    POST https://www3.sii.cl/getPublicacionesCTByMateria
    Body JSON: {"key":"IVA","year":"2026"}
    Fuente confirmada: HTML fuente del portal SII
    """
    try:
        r = SESSION.post(
            f"{SII_API}/getPublicacionesCTByMateria",
            data=json.dumps({"key": api_key, "year": str(anio)}),
            headers={
                'Content-Type': 'application/json',
                'Referer': 'https://www.sii.cl/normativa_legislacion/jurisprudencia_administrativa/',
                'Origin': 'https://www.sii.cl',
            },
            timeout=25
        )
        if r.status_code == 200:
            data = r.json()
            log.info(f"  API [{api_key}/{anio}]: {len(data)} oficios")
            return data
        log.warning(f"  API HTTP {r.status_code} para key={api_key} year={anio}")
        return []
    except Exception as e:
        log.error(f"  Error API [{api_key}/{anio}]: {e}")
        return []

def descargar_pdf_oficio(id_blob, nombre_doc, extension, media_type):
    """
    Descarga PDF de oficio via form POST al portal SII.
    URL verificada con DevTools: https://www4.sii.cl/gabineteAdmInternet/descargaArchivo
    Equivalente a abreDoctoJurAdm() del JS del portal.
    Campos del form: nombreDocumento, extension, acc, id, mediaType
    """
    # Calentar sesión visitando la página padre para obtener cookies SII
    try:
        SESSION.get(
            'https://www.sii.cl/normativa_legislacion/jurisprudencia_administrativa/indice_jadm.htm',
            timeout=10
        )
    except:
        pass

    payload = {
        'nombreDocumento': nombre_doc,
        'extension': extension,
        'acc': 'download',
        'id': id_blob,
        'mediaType': media_type,
    }
    hdrs = {
        'Referer': 'https://www.sii.cl/normativa_legislacion/jurisprudencia_administrativa/',
        'Origin': 'https://www.sii.cl',
    }

    try:
        r = SESSION.get(
            DESCARGA_URL,
            params=payload,
            headers=hdrs,
            timeout=30
        )
        log.info(f"  blob status={r.status_code} size={len(r.content)} ct={r.headers.get('Content-Type','')[:60]}")
        if r.status_code == 200 and len(r.content) > 500:
            return r.content
        else:
            log.warning(f"  blob respuesta inesperada: {r.text[:200]}")
    except Exception as e:
        log.error(f"  error descarga blob: {e}")

    return None

def descargar_pdf(url, retries=3, delay=1.0):
    for i in range(retries):
        try:
            r = SESSION.get(url, timeout=20)
            if r.status_code == 200 and len(r.content) > 1000: return r.content
            if r.status_code == 404: return None
        except requests.exceptions.RequestException as e:
            log.warning(f"  Intento {i+1}/{retries}: {e}")
            if i < retries-1: time.sleep(delay*(i+1))
    return None

# ── Procesar PDF descargado ────────────────────────────────────────────────
def _procesar_y_guardar(pdf_bytes, tipo_norm, tipo_clave, numero, anio,
                         doc_info, url_ref, callback, i, total):
    # Guardar en disco
    pdf_categoria = oficio_pdf_categoria(tipo_clave, doc_info) if tipo_norm == 'oficio' else None
    blob_suffix = ""
    if tipo_norm == 'oficio':
        blob_id = (doc_info.get('_blob_id') or '').strip()
        if blob_id:
            blob_suffix = f"_{blob_id[:8]}"
    pdf_path = build_pdf_path(
        tipo_norm,
        anio,
        f"{tipo_norm}_{anio}_{numero.zfill(4)}{blob_suffix}.pdf",
        categoria=pdf_categoria,
    )
    try:
        with open(pdf_path, 'wb') as f: f.write(pdf_bytes)
    except: pass

    ext = extraer_texto_pdf(pdf_bytes)
    if not ext['ok'] or ext['chars'] < 50:
        log_scraper(tipo_clave, anio, numero, 'extraccion_fallida', url_ref)
        return False, False

    texto = ext['texto']
    hash_doc = hashlib.md5(pdf_bytes).hexdigest()
    if doc_existe_hash(hash_doc): return True, False  # duplicado

    leyes   = detectar_leyes(texto)
    arts    = detectar_articulos(texto)
    if tipo_norm == 'oficio':
        fecha = (
            doc_info.get('fecha')
            or extraer_fecha_oficio(texto, numero)
            or extraer_fecha_texto(texto)
            or f"{anio}-01-01"
        )
    else:
        fecha = extraer_fecha_texto(texto) or doc_info.get('fecha', f"{anio}-01-01")
    resumen = extraer_resumen(texto)

    titulo = (doc_info.get('titulo') or doc_info.get('pubLegal') or doc_info.get('descripcion') or '')
    if len(titulo) < 5:
        mapa = {'circular':'Circular','oficio':'Oficio Ordinario','resolucion':'Resolución Ex.'}
        titulo = f"{mapa.get(tipo_norm, tipo_norm)} N°{numero} de {anio}"

    if tipo_norm == 'oficio':
        referencia = f"Oficio Ordinario N°{numero}, de {fecha}"
        subtema = f"{oficio_subtema_prefijo(tipo_clave, doc_info)} — {doc_info.get('pubLegal','')[:250]}"
    elif tipo_norm == 'circular':
        referencia = f"Circular N°{numero} de {anio}"
        subtema = doc_info.get('descripcion','')[:200]
    else:
        referencia = f"Resolución Ex. N°{numero} de {anio}"
        subtema = doc_info.get('descripcion','')[:200]

    doc_data = {
        'hash_md5': hash_doc,
        'tipo': tipo_norm,
        'numero': numero,
        'anio': anio,
        'fecha': fecha,
        'titulo': titulo[:500],
        'materia': None,
        'subtema': (subtema or '')[:300],
        'contenido': texto,
        'resumen': resumen,
        'url_sii': url_ref,
        'referencia': referencia,
        'palabras_clave': None,
        'leyes_citadas': json.dumps(leyes),
        'articulos_clave': json.dumps(arts[:20]),
        'paginas': ext['paginas'],
        'chars_texto': ext['chars'],
        'pdf_local': os.path.relpath(pdf_path, BASE).replace('\\', '/'),
        'pdf_size_bytes': len(pdf_bytes),
        'fuente': 'scraper',
    }

    doc_id = guardar_documento(doc_data)
    if doc_id:
        log_scraper(tipo_clave, anio, numero, 'ok', url_ref)
        log.info(f"  OK [{i+1}/{total}] {referencia} — {ext['paginas']} págs")
        if callback: callback(f"OK [{i+1}/{total}] {referencia} ({ext['paginas']} págs)", True, total)
        return True, True
    else:
        log_scraper(tipo_clave, anio, numero, 'error_bd', url_ref)
        return False, False

# ── Scraper principal ─────────────────────────────────────────────────────
def scrape_anio(tipo_clave, anio, callback=None, delay=0.8):
    """
    Scrape un tipo+año completo.
    Oficios: API JSON + blob download.
    Circulares/Resoluciones: índice HTML + PDF directo.
    """
    tipo_norm = tipo_clave.split('_')[0]
    log.info(f"=== {tipo_clave.upper()} {anio} ===")

    # ── OFICIOS: API JSON ─────────────────────────────────────────────────
    if tipo_norm == 'oficio':
        api_key = OFICIO_API_KEYS.get(tipo_clave)
        if not api_key:
            msg = f"Tipo desconocido: {tipo_clave}. Usar: oficio_iva, oficio_lir, oficio_otras"
            log.error(msg)
            if callback: callback(msg, False, 0)
            return {'ok': False, 'tipo': tipo_clave, 'anio': anio, 'total': 0, 'nuevos': 0, 'saltados': 0, 'errores': 0}

        ensure_oficio_fuentes_table()

        if callback: callback(f"API SII: key={api_key}, year={anio}...", True, 0)
        items = obtener_oficios_api(api_key, anio)

        if not items:
            msg = f"Sin datos en API para key={api_key} year={anio}"
            log.warning(f"  {msg}")
            if callback: callback(msg, False, 0)
            return {'ok': False, 'tipo': tipo_clave, 'anio': anio, 'total': 0, 'nuevos': 0, 'saltados': 0, 'errores': 0}

        total = len(items)
        nuevos = errores = saltados = 0
        if callback: callback(f"LISTA {api_key}/{anio}: {total} oficios", True, total)

        for i, item in enumerate(items):
            numero    = str(item.get('pubNumOficio', '')).strip()
            id_blob   = str(item.get('idBlobArchPublica', '')).strip()
            extension = str(item.get('extensionArchPublica', 'pdf'))
            mtype     = str(item.get('mTypeArchPublica', 'application/pdf'))
            fecha_pub = str(item.get('pubFechaPubli', f"01/01/{anio}"))
            nombre_doc = f"{numero}-{fecha_pub}.{extension}"

            if not numero:
                errores += 1; continue

            if not id_blob or id_blob in ('None', '0', ''):
                errores += 1
                log.warning(f"  Oficio N°{numero}/{anio}: sin blob ID")
                continue

            url_ref = f"{SII_API}/accesoADoctosCT?id={id_blob}"
            existing_by_url = doc_id_por_url(url_ref)
            if existing_by_url:
                registrar_oficio_fuente(existing_by_url, anio, api_key, numero, id_blob, url_ref, _convertir_fecha(fecha_pub, anio))
                saltados += 1
                if saltados % 20 == 0 and callback:
                    callback(f"[{i+1}/{total}] Ya indexados: {saltados}", True, total)
                continue

            pdf_bytes = descargar_pdf_oficio(id_blob, nombre_doc, extension, mtype)

            if pdf_bytes is None:
                errores += 1
                log_scraper(tipo_clave, anio, numero, 'blob_fallido', url_ref)
                if callback: callback(f"WARN [{i+1}/{total}] Oficio N°{numero}/{anio} - descarga fallida", False, total)
                time.sleep(delay * 0.5)
                continue

            hash_doc = hashlib.md5(pdf_bytes).hexdigest()
            existing_by_hash = doc_id_por_hash(hash_doc)
            if existing_by_hash:
                registrar_oficio_fuente(existing_by_hash, anio, api_key, numero, id_blob, url_ref, _convertir_fecha(fecha_pub, anio))
                saltados += 1
                continue

            doc_info = {
                '_api_key':    api_key,
                '_blob_id':    id_blob,
                'titulo':      item.get('pubLegal', ''),
                'pubLegal':    item.get('pubLegal', ''),
                'descripcion': item.get('pubResumen', ''),
                'fecha':       _convertir_fecha(fecha_pub, anio),
            }
            ok, nuevo = _procesar_y_guardar(
                pdf_bytes, 'oficio', tipo_clave, numero, anio,
                doc_info, url_ref, callback, i, total
            )
            if ok:
                doc_id = doc_id_por_url(url_ref) or doc_id_por_hash(hashlib.md5(pdf_bytes).hexdigest())
                registrar_oficio_fuente(doc_id, anio, api_key, numero, id_blob, url_ref, _convertir_fecha(fecha_pub, anio))
            if nuevo:    nuevos += 1
            elif not ok: errores += 1
            else:        saltados += 1
            time.sleep(delay)

        resumen = {'ok': True, 'tipo': tipo_clave, 'anio': anio,
                   'total': total, 'nuevos': nuevos, 'saltados': saltados, 'errores': errores}
        log.info(f"=== FIN {tipo_clave.upper()} {anio}: {nuevos} nuevos, {saltados} existían, {errores} errores ===")
        if callback: callback(f"DONE|{nuevos}|{errores}|{total}", True, total)
        return resumen

    # ── CIRCULARES / RESOLUCIONES: índice HTML ────────────────────────────
    if tipo_clave not in INDICES:
        msg = f"Tipo desconocido: {tipo_clave}"
        if callback: callback(msg, False, 0)
        return {'ok': False, 'total': 0, 'nuevos': 0, 'errores': 0}

    urls_indice = obtener_urls_indice(tipo_clave, anio)
    url_indice = urls_indice[0]
    html = None
    if callback: callback(f"Descargando indice {tipo_clave} {anio}...", True, 0)

    for candidata in urls_indice:
        log.info(f"  Indice: {candidata}")
        try:
            r = SESSION.get(candidata, timeout=15)
            if r.status_code != 200:
                log.warning(f"  Indice HTTP {r.status_code}: {candidata}")
                continue
            r.encoding = 'utf-8'
            html = r.text
            url_indice = candidata
            break
        except Exception as e:
            log.warning(f"  Error indice {candidata}: {e}")

    if html is None:
        msg = f"No se pudo descargar indice para {tipo_clave}/{anio}"
        if callback: callback(msg, False, 0)
        return {'ok': False, 'total': 0, 'nuevos': 0, 'errores': 0}
    docs = parsear_indice_circulares(html, anio, url_indice) if tipo_clave == 'circular' else parsear_indice_resoluciones(html, anio)
    total = len(docs)
    log.info(f"  {total} documentos en índice")
    if callback: callback(f"LISTA {total} documentos en indice {anio}", True, total)

    nuevos = errores = saltados = 0
    for i, doc_info in enumerate(docs):
        numero = doc_info['numero']
        if doc_existe(tipo_clave, numero, anio):
            saltados += 1
            if saltados % 20 == 0 and callback:
                callback(f"[{i+1}/{total}] Ya indexados: {saltados}", True, total)
            continue

        url_pdf = doc_info.get('url_pdf')
        if not url_pdf and doc_info.get('url_detalle'):
            url_pdf = _resolver_url_pdf_desde_detalle(doc_info['url_detalle'])
        if not url_pdf:
            for candidata in obtener_urls_pdf(tipo_clave, anio, numero):
                url_pdf = candidata
                pdf_bytes = descargar_pdf(url_pdf, delay=delay)
                if pdf_bytes is not None:
                    break
            else:
                pdf_bytes = None
        else:
            pdf_bytes = descargar_pdf(url_pdf, delay=delay)

        url_ref = doc_info.get('url_detalle') or url_pdf

        if pdf_bytes is None and tipo_clave == 'circular' and doc_info.get('url_detalle'):
            html_doc = _descargar_circular_html(doc_info['url_detalle'], anio, doc_info)
            if html_doc:
                pdf_bytes = html_doc['pdf_bytes']
                doc_info = dict(doc_info)
                doc_info['titulo'] = html_doc.get('titulo') or doc_info.get('titulo')
                doc_info['descripcion'] = html_doc.get('descripcion') or doc_info.get('descripcion')
                doc_info['fecha'] = html_doc.get('fecha') or doc_info.get('fecha')

        if pdf_bytes is None:
            errores += 1
            log_scraper(tipo_clave, anio, numero, 'no_encontrado', url_pdf)
            if i % 10 == 0 and callback:
                callback(f"[WARN] [{i+1}/{total}] N{numero}/{anio} no encontrado", False, total)
            time.sleep(delay * 0.5)
            continue

        ok, nuevo = _procesar_y_guardar(
            pdf_bytes, tipo_norm, tipo_clave, numero, anio,
            doc_info, url_ref, callback, i, total
        )
        if nuevo:    nuevos += 1
        elif not ok: errores += 1
        else:        saltados += 1
        time.sleep(delay)

    resumen = {'ok': True, 'tipo': tipo_clave, 'anio': anio,
               'total': total, 'nuevos': nuevos, 'saltados': saltados, 'errores': errores}
    log.info(f"=== FIN {tipo_clave.upper()} {anio}: {nuevos} nuevos, {saltados} existían, {errores} errores ===")
    if callback: callback(f"DONE|{nuevos}|{errores}|{total}", True, total)
    return resumen

# ── Scraper masivo ────────────────────────────────────────────────────────
def scrape_historico(tipos, anio_desde, anio_hasta, delay=1.0, callback=None):
    log.info(f"HISTÓRICO: {tipos} {anio_desde}-{anio_hasta}")
    resultados = []
    for anio in range(anio_hasta, anio_desde-1, -1):
        for tipo in tipos:
            try:
                r = scrape_anio(tipo, anio, callback=callback, delay=delay)
                resultados.append(r)
                time.sleep(2)
            except Exception as e:
                log.error(f"Error {tipo}/{anio}: {e}")
    total = sum(r.get('nuevos',0) for r in resultados)
    log.info(f"HISTÓRICO COMPLETADO — {total} documentos nuevos")
    return resultados

# ── Scheduler diario ──────────────────────────────────────────────────────
def check_novedades(callback=None):
    anio = date.today().year
    tipos = ['circular', 'resolucion', 'oficio_iva', 'oficio_lir', 'oficio_otras']
    log.info(f"Verificando novedades {anio}...")
    if callback: callback(f"Verificando novedades {anio}...")
    total = 0
    for tipo in tipos:
        try:
            r = scrape_anio(tipo, anio, callback=callback, delay=0.8)
            total += r.get('nuevos', 0)
        except Exception as e:
            log.error(f"Error {tipo}: {e}")
    log.info(f"OK {total} documentos nuevos")
    if callback: callback(f"DAILY_DONE|{total}")
    return total

def iniciar_scheduler():
    try:
        import schedule, threading
        def job():
            log.info("Verificacion diaria")
            check_novedades()
        schedule.every().day.at("08:00").do(job)
        def run():
            log.info("Scheduler activo - 08:00 diario")
            while True:
                schedule.run_pending()
                time.sleep(60)
        t = threading.Thread(target=run, daemon=True)
        t.start()
        return t
    except ImportError:
        log.warning("Instalar: pip install schedule")
        return None

# ── CLI ────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='SII Normativa Scraper v2.1')
    parser.add_argument('--tipo', default='circular',
                        choices=['circular','resolucion','oficio_iva','oficio_lir','oficio_otras'])
    parser.add_argument('--desde', type=int, default=2020)
    parser.add_argument('--hasta', type=int, default=date.today().year)
    parser.add_argument('--delay', type=float, default=1.0)
    parser.add_argument('--historico', action='store_true')
    args = parser.parse_args()

    if args.historico:
        scrape_historico(['circular','resolucion','oficio_iva','oficio_lir','oficio_otras'],
                         args.desde, args.hasta, delay=args.delay)
    else:
        for anio in range(args.hasta, args.desde-1, -1):
            scrape_anio(args.tipo, anio, delay=args.delay)


