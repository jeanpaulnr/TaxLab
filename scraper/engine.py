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

import os, sys, time, json, hashlib, re, logging
import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import fitz  # PyMuPDF

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

# Claves verificadas en el HTML fuente del portal SII
OFICIO_API_KEYS = {
    'oficio_iva':   'IVA',    # Ley Impuesto Ventas y Servicios
    'oficio_lir':   'RENTA',  # Ley Impuesto a la Renta
    'oficio_otras': 'OTRAS',  # Otras Normas (CT, Timbres, Herencias...)
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

def doc_existe(tipo, numero, anio):
    tipo_n = tipo.split('_')[0]
    conn = get_db()
    r = conn.execute("SELECT id FROM documentos WHERE tipo=? AND numero=? AND anio=?",
                     (tipo_n, numero, anio)).fetchone()
    conn.close()
    return r is not None

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
def guardar_documento(data):
    conn = get_db()
    try:
        conn.execute('''INSERT OR IGNORE INTO documentos
            (hash_md5, tipo, numero, anio, fecha, titulo, materia, subtema,
             contenido, resumen, url_sii, referencia, palabras_clave,
             leyes_citadas, articulos_clave, fuente)
            VALUES
            (:hash_md5, :tipo, :numero, :anio, :fecha, :titulo, :materia, :subtema,
             :contenido, :resumen, :url_sii, :referencia, :palabras_clave,
             :leyes_citadas, :articulos_clave, :fuente)''', data)
        row = conn.execute("SELECT id FROM documentos WHERE hash_md5=?", (data['hash_md5'],)).fetchone()
        doc_id = row['id'] if row else None
        if doc_id:
            for ley in json.loads(data.get('leyes_citadas','[]')):
                for art in json.loads(data.get('articulos_clave','[]'))[:20]:
                    try: conn.execute("INSERT OR IGNORE INTO articulos_idx(doc_id,ley,articulo) VALUES(?,?,?)",(doc_id,ley,art))
                    except: pass
        conn.commit()
        return doc_id
    except Exception as e:
        log.error(f"Error BD: {e}"); return None
    finally: conn.close()

def log_scraper(tipo, anio, numero, estado, url):
    conn = get_db()
    try:
        conn.execute("INSERT INTO scraper_log(tipo,anio,numero,estado,url) VALUES(?,?,?,?,?)",(tipo,anio,numero,estado,url))
        conn.commit()
    except: pass
    finally: conn.close()

# ── Parseo índices HTML ───────────────────────────────────────────────────
def parsear_indice_circulares(html, anio):
    soup = BeautifulSoup(html, 'html.parser')
    docs = []
    for h5 in soup.find_all('h5'):
        a = h5.find('a')
        if not a: continue
        m = re.search(r'circu(\d+)\.pdf', a.get('href',''), re.IGNORECASE)
        if not m: continue
        MESES = {'enero':'01','febrero':'02','marzo':'03','abril':'04','mayo':'05',
                 'junio':'06','julio':'07','agosto':'08','septiembre':'09',
                 'octubre':'10','noviembre':'11','diciembre':'12'}
        mf = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+del?\s+(\d{4})', h5.get_text())
        fecha = f"{anio}-01-01"
        if mf:
            d,mes,y = mf.groups()
            mn = MESES.get(mes.lower())
            if mn: fecha = f"{y}-{mn}-{d.zfill(2)}"
        sig = h5.find_next_sibling()
        desc = sig.get_text(strip=True) if sig and sig.name=='p' else ''
        docs.append({'numero': m.group(1), 'titulo': a.get_text(strip=True), 'descripcion': desc, 'fecha': fecha})
    return docs

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
    pdf_path = os.path.join(PDF_DIR, f"{tipo_norm}_{anio}_{numero.zfill(4)}.pdf")
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
    fecha   = extraer_fecha_texto(texto) or doc_info.get('fecha', f"{anio}-01-01")
    resumen = extraer_resumen(texto)

    titulo = (doc_info.get('titulo') or doc_info.get('pubLegal') or doc_info.get('descripcion') or '')
    if len(titulo) < 5:
        mapa = {'circular':'Circular','oficio':'Oficio Ordinario','resolucion':'Resolución Ex.'}
        titulo = f"{mapa.get(tipo_norm, tipo_norm)} N°{numero} de {anio}"

    if tipo_norm == 'oficio':
        referencia = f"Oficio Ordinario N°{numero}, de {fecha}"
        subtema = f"{doc_info.get('_api_key','')} — {doc_info.get('pubLegal','')[:250]}"
    elif tipo_norm == 'circular':
        referencia = f"Circular N°{numero} de {anio}"
        subtema = doc_info.get('descripcion','')[:200]
    else:
        referencia = f"Resolución Ex. N°{numero} de {anio}"
        subtema = doc_info.get('descripcion','')[:200]

    doc_data = {
        'hash_md5': hash_doc, 'tipo': tipo_norm, 'numero': numero, 'anio': anio,
        'fecha': fecha, 'titulo': titulo[:500], 'materia': None,
        'subtema': (subtema or '')[:300], 'contenido': texto[:50000],
        'resumen': resumen, 'url_sii': url_ref, 'referencia': referencia,
        'palabras_clave': None, 'leyes_citadas': json.dumps(leyes),
        'articulos_clave': json.dumps(arts[:20]), 'fuente': 'scraper',
    }

    doc_id = guardar_documento(doc_data)
    if doc_id:
        log_scraper(tipo_clave, anio, numero, 'ok', url_ref)
        log.info(f"  ✅ [{i+1}/{total}] {referencia} — {ext['paginas']} págs")
        if callback: callback(f"✅ [{i+1}/{total}] {referencia} ({ext['paginas']} págs)", True, total)
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

        if callback: callback(f"🔍 API SII: key={api_key}, year={anio}...", True, 0)
        items = obtener_oficios_api(api_key, anio)

        if not items:
            msg = f"Sin datos en API para key={api_key} year={anio}"
            log.warning(f"  {msg}")
            if callback: callback(msg, False, 0)
            return {'ok': False, 'tipo': tipo_clave, 'anio': anio, 'total': 0, 'nuevos': 0, 'saltados': 0, 'errores': 0}

        total = len(items)
        nuevos = errores = saltados = 0
        if callback: callback(f"📋 {api_key}/{anio}: {total} oficios", True, total)

        for i, item in enumerate(items):
            numero    = str(item.get('pubNumOficio', '')).strip()
            id_blob   = str(item.get('idBlobArchPublica', '')).strip()
            extension = str(item.get('extensionArchPublica', 'pdf'))
            mtype     = str(item.get('mTypeArchPublica', 'application/pdf'))
            fecha_pub = str(item.get('pubFechaPubli', f"01/01/{anio}"))
            nombre_doc = f"{numero}-{fecha_pub}.{extension}"

            if not numero:
                errores += 1; continue

            if doc_existe('oficio', numero, anio):
                saltados += 1
                if saltados % 20 == 0 and callback:
                    callback(f"[{i+1}/{total}] Ya indexados: {saltados}", True, total)
                continue

            if not id_blob or id_blob in ('None', '0', ''):
                errores += 1
                log.warning(f"  Oficio N°{numero}/{anio}: sin blob ID")
                continue

            url_ref = f"{SII_API}/accesoADoctosCT?id={id_blob}"
            pdf_bytes = descargar_pdf_oficio(id_blob, nombre_doc, extension, mtype)

            if pdf_bytes is None:
                errores += 1
                log_scraper(tipo_clave, anio, numero, 'blob_fallido', url_ref)
                if callback: callback(f"⚠ [{i+1}/{total}] Oficio N°{numero}/{anio} — descarga fallida", False, total)
                time.sleep(delay * 0.5)
                continue

            doc_info = {
                '_api_key':    api_key,
                'titulo':      item.get('pubLegal', ''),
                'pubLegal':    item.get('pubLegal', ''),
                'descripcion': item.get('pubResumen', ''),
                'fecha':       _convertir_fecha(fecha_pub, anio),
            }
            ok, nuevo = _procesar_y_guardar(
                pdf_bytes, 'oficio', tipo_clave, numero, anio,
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

    # ── CIRCULARES / RESOLUCIONES: índice HTML ────────────────────────────
    if tipo_clave not in INDICES:
        msg = f"Tipo desconocido: {tipo_clave}"
        if callback: callback(msg, False, 0)
        return {'ok': False, 'total': 0, 'nuevos': 0, 'errores': 0}

    url_indice = INDICES[tipo_clave].format(anio=anio)
    log.info(f"  Índice: {url_indice}")
    if callback: callback(f"🔍 Descargando índice {tipo_clave} {anio}...", True, 0)

    try:
        r = SESSION.get(url_indice, timeout=15)
        if r.status_code != 200:
            msg = f"Índice HTTP {r.status_code} — no existe para {anio}"
            log.warning(f"  {msg}")
            if callback: callback(msg, False, 0)
            return {'ok': False, 'total': 0, 'nuevos': 0, 'errores': 0}
        r.encoding = 'utf-8'
        html = r.text
    except Exception as e:
        log.error(f"  Error índice: {e}")
        if callback: callback(str(e), False, 0)
        return {'ok': False, 'total': 0, 'nuevos': 0, 'errores': 0}

    docs = parsear_indice_circulares(html, anio) if tipo_clave == 'circular' else parsear_indice_resoluciones(html, anio)
    total = len(docs)
    log.info(f"  {total} documentos en índice")
    if callback: callback(f"📋 {total} documentos en índice {anio}", True, total)

    nuevos = errores = saltados = 0
    for i, doc_info in enumerate(docs):
        numero = doc_info['numero']
        if doc_existe(tipo_clave, numero, anio):
            saltados += 1
            if saltados % 20 == 0 and callback:
                callback(f"[{i+1}/{total}] Ya indexados: {saltados}", True, total)
            continue

        url_pdf = PDF_URLS[tipo_clave].format(anio=anio, num=numero)
        pdf_bytes = descargar_pdf(url_pdf, delay=delay)

        if pdf_bytes is None:
            errores += 1
            log_scraper(tipo_clave, anio, numero, 'no_encontrado', url_pdf)
            if i % 10 == 0 and callback:
                callback(f"⚠ [{i+1}/{total}] N°{numero}/{anio} no encontrado", False, total)
            time.sleep(delay * 0.5)
            continue

        ok, nuevo = _procesar_y_guardar(
            pdf_bytes, tipo_norm, tipo_clave, numero, anio,
            doc_info, url_pdf, callback, i, total
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
    log.info(f"⏰ Verificando novedades {anio}...")
    if callback: callback(f"Verificando novedades {anio}...")
    total = 0
    for tipo in tipos:
        try:
            r = scrape_anio(tipo, anio, callback=callback, delay=0.8)
            total += r.get('nuevos', 0)
        except Exception as e:
            log.error(f"Error {tipo}: {e}")
    log.info(f"✅ {total} documentos nuevos")
    if callback: callback(f"DAILY_DONE|{total}")
    return total

def iniciar_scheduler():
    try:
        import schedule, threading
        def job():
            log.info("⏰ Verificación diaria")
            check_novedades()
        schedule.every().day.at("08:00").do(job)
        def run():
            log.info("✅ Scheduler activo — 08:00 diario")
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