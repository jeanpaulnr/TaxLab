"""
SII Normativa V2.0 — Sistema completo con scraping real
Incluye: Flask app + scheduler diario + panel de control
"""

from flask import Flask, render_template, request, jsonify, Response, stream_with_context
import sqlite3, os, json, re, hashlib, threading, logging
from datetime import datetime, date

app = Flask(__name__)
BASE = os.path.dirname(os.path.abspath(__file__))
DB   = os.path.join(BASE, 'data', 'sii_normativa.db')

# ── Logging ──────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('sii_app')

# ── Estado global del scraper ─────────────────────────────────────────────
scraper_status = {
    'running': False,
    'messages': [],
    'total': 0,
    'procesados': 0,
    'nuevos': 0,
    'errores': 0,
    'inicio': None,
}
_lock = threading.Lock()

def push_msg(msg: str, ok: bool = True, total: int = 0):
    with _lock:
        scraper_status['messages'].append({
            'msg': msg, 'ok': ok, 'ts': datetime.now().isoformat()
        })
        if total > 0: scraper_status['total'] = total
        if msg.startswith('✅'): scraper_status['nuevos'] += 1
        if msg.startswith('⚠'): scraper_status['errores'] += 1
        scraper_status['procesados'] += 1
        if len(scraper_status['messages']) > 500:
            scraper_status['messages'] = scraper_status['messages'][-300:]

# ── Schema ───────────────────────────────────────────────────────────────
SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS documentos (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    hash_md5        TEXT UNIQUE,
    tipo            TEXT NOT NULL,
    numero          TEXT,
    anio            INTEGER,
    fecha           TEXT,
    titulo          TEXT NOT NULL,
    materia         TEXT,
    subtema         TEXT,
    contenido       TEXT,
    resumen         TEXT,
    url_sii         TEXT,
    referencia      TEXT,
    palabras_clave  TEXT,
    leyes_citadas   TEXT,
    articulos_clave TEXT,
    paginas         INTEGER DEFAULT 0,
    chars_texto     INTEGER DEFAULT 0,
    vigente         INTEGER DEFAULT 1,
    reemplazado_por TEXT,
    fecha_carga     TEXT DEFAULT (datetime('now')),
    fuente          TEXT DEFAULT 'manual'
);
CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts USING fts5(
    titulo, materia, subtema, contenido, resumen,
    palabras_clave, leyes_citadas, articulos_clave,
    content='documentos', content_rowid='id',
    tokenize='unicode61 remove_diacritics 1'
);
CREATE TRIGGER IF NOT EXISTS docs_ai AFTER INSERT ON documentos BEGIN
    INSERT INTO docs_fts(rowid,titulo,materia,subtema,contenido,resumen,
                         palabras_clave,leyes_citadas,articulos_clave)
    VALUES(new.id,new.titulo,new.materia,new.subtema,new.contenido,new.resumen,
           new.palabras_clave,new.leyes_citadas,new.articulos_clave);
END;
CREATE TRIGGER IF NOT EXISTS docs_ad AFTER DELETE ON documentos BEGIN
    INSERT INTO docs_fts(docs_fts,rowid,titulo,materia,subtema,contenido,resumen,
                         palabras_clave,leyes_citadas,articulos_clave)
    VALUES('delete',old.id,old.titulo,old.materia,old.subtema,old.contenido,old.resumen,
           old.palabras_clave,old.leyes_citadas,old.articulos_clave);
END;
CREATE TABLE IF NOT EXISTS articulos_idx (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id     INTEGER REFERENCES documentos(id) ON DELETE CASCADE,
    ley        TEXT,
    articulo   TEXT
);
CREATE INDEX IF NOT EXISTS idx_art_ley  ON articulos_idx(ley, articulo);
CREATE INDEX IF NOT EXISTS idx_doc_tipo ON documentos(tipo, anio);
CREATE INDEX IF NOT EXISTS idx_doc_hash ON documentos(hash_md5);
CREATE TABLE IF NOT EXISTS historial (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    termino    TEXT,
    filtros    TEXT,
    fecha      TEXT DEFAULT (datetime('now')),
    resultados INTEGER
);
CREATE TABLE IF NOT EXISTS scraper_log (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo     TEXT, anio INTEGER, numero TEXT,
    estado   TEXT, url TEXT,
    fecha    TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS scheduler_config (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

MATERIAS = [
    'IVA','Renta','Timbre','Herencias','Bienes Raíces','Ganancias de Capital',
    'Gastos Necesarios','Pro Pyme','Renta Atribuida','Régimen Semi Integrado',
    'FUT','RAI','SAC','DDAN','REX','Impuesto Adicional',
    'Impuesto Global Complementario','Segunda Categoría','Teletrabajo',
    'Servicios Digitales','Exportaciones','Facturas','Contabilidad',
    'Depreciación','Corrección Monetaria','Fiscalización','Tasación',
    'Citación','Liquidación','Giro','Prescripción','Recursos',
    'Tribunal Tributario','PPMO','PPM','F29','F22',
]

LEYES = ['LIR','LIVS','CT','LTE','LH','LMT','LRT']

def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    conn = get_db()
    for stmt in SCHEMA.split(';'):
        s = stmt.strip()
        if s:
            try: conn.execute(s)
            except Exception as e:
                if 'already exists' not in str(e): pass
    conn.commit(); conn.close()
    log.info("✅ Base de datos inicializada")

def tipo_formal(tipo):
    return {'circular':'Circular','oficio':'Oficio','resolucion':'Resolución Exenta'}.get(tipo, tipo.capitalize())

# ── Rutas principales ─────────────────────────────────────────────────────
@app.route('/')
def index():
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM documentos"); total = c.fetchone()[0]
    c.execute("SELECT tipo,COUNT(*) cnt FROM documentos GROUP BY tipo")
    por_tipo = {r['tipo']:r['cnt'] for r in c.fetchall()}
    c.execute("SELECT anio,COUNT(*) cnt FROM documentos WHERE anio IS NOT NULL GROUP BY anio ORDER BY anio DESC LIMIT 15")
    por_anio = [dict(r) for r in c.fetchall()]
    c.execute("SELECT termino,resultados FROM historial ORDER BY fecha DESC LIMIT 8")
    recientes = [dict(r) for r in c.fetchall()]
    c.execute("SELECT ley,articulo,COUNT(*) cnt FROM articulos_idx GROUP BY ley,articulo ORDER BY cnt DESC LIMIT 15")
    top_arts = [dict(r) for r in c.fetchall()]
    c.execute("SELECT MAX(fecha_carga) FROM documentos WHERE fuente='scraper'")
    ultima_actualizacion = c.fetchone()[0]
    conn.close()
    return render_template('index.html', total=total, por_tipo=por_tipo,
        por_anio=por_anio, recientes=recientes, top_arts=top_arts,
        materias=MATERIAS, leyes=LEYES,
        ultima_actualizacion=ultima_actualizacion)

@app.route('/buscar')
def buscar():
    q       = request.args.get('q','').strip()
    tipo    = request.args.get('tipo','')
    anio    = request.args.get('anio','')
    ley     = request.args.get('ley','')
    materia = request.args.get('materia','')
    art     = request.args.get('articulo','').strip()
    page    = max(1, int(request.args.get('page', 1)))
    per     = 15

    conn = get_db(); c = conn.cursor()
    where, params = [], []

    if q:
        try:
            fts_q = ' OR '.join(f'"{w}"' if len(w)>3 else w for w in q.split())
            c.execute('SELECT rowid FROM docs_fts WHERE docs_fts MATCH ? ORDER BY rank LIMIT 500', (fts_q,))
            ids = [r[0] for r in c.fetchall()]
        except:
            ids = []
        if ids:
            where.append(f"d.id IN ({','.join(['?']*len(ids))})")
            params.extend(ids)
        else:
            like = f'%{q}%'
            where.append("(d.titulo LIKE ? OR d.contenido LIKE ? OR d.palabras_clave LIKE ? OR d.resumen LIKE ?)")
            params.extend([like]*4)

    if tipo:    where.append("d.tipo=?");               params.append(tipo)
    if anio:    where.append("d.anio=?");               params.append(int(anio))
    if ley:     where.append("d.leyes_citadas LIKE ?"); params.append(f'%"{ley}"%')
    if materia: where.append("d.materia LIKE ?");       params.append(f'%{materia}%')
    if art:     where.append("d.articulos_clave LIKE ?"); params.append(f'%{art}%')

    clause = ("WHERE " + " AND ".join(where)) if where else ""

    c.execute(f"SELECT COUNT(*) FROM documentos d {clause}", params)
    total = c.fetchone()[0]

    c.execute(f"""
        SELECT d.id,d.tipo,d.numero,d.anio,d.fecha,d.titulo,d.materia,
               d.referencia,d.resumen,d.leyes_citadas,d.articulos_clave,
               d.vigente,d.paginas,d.chars_texto,
               SUBSTR(d.contenido,1,500) extracto
        FROM documentos d {clause}
        ORDER BY d.anio DESC, CAST(d.numero AS INTEGER) DESC
        LIMIT ? OFFSET ?
    """, params + [per, (page-1)*per])
    rows = c.fetchall()

    if q or tipo or ley or art:
        try:
            c.execute("INSERT INTO historial(termino,filtros,resultados) VALUES(?,?,?)",
                      (q, json.dumps({'tipo':tipo,'ley':ley,'art':art}), total))
            conn.commit()
        except: pass
    conn.close()

    resultados = []
    for r in rows:
        extracto = r['resumen'] or r['extracto'] or ''
        if q:
            for word in q.split():
                if len(word) > 2:
                    extracto = re.sub(f'(?i)({re.escape(word)})', r'<mark>\1</mark>', extracto)
        resultados.append({
            'id': r['id'], 'tipo': r['tipo'], 'numero': r['numero'],
            'anio': r['anio'], 'fecha': r['fecha'], 'titulo': r['titulo'],
            'materia': r['materia'], 'referencia': r['referencia'],
            'extracto': extracto[:500] + ('…' if len(extracto) > 500 else ''),
            'leyes': json.loads(r['leyes_citadas'] or '[]'),
            'articulos': json.loads(r['articulos_clave'] or '[]')[:5],
            'vigente': r['vigente'],
            'paginas': r['paginas'] or 0,
        })

    return jsonify({'resultados': resultados, 'total': total,
                    'pagina': page, 'paginas': max(1, (total+per-1)//per)})

@app.route('/documento/<int:doc_id>')
def ver_documento(doc_id):
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT * FROM documentos WHERE id=?", (doc_id,))
    row = c.fetchone()
    if not row:
        return "Documento no encontrado", 404
    doc = dict(row)
    doc['leyes_citadas']   = json.loads(doc.get('leyes_citadas') or '[]')
    doc['articulos_clave'] = json.loads(doc.get('articulos_clave') or '[]')

    # Relacionados por artículos
    arts = doc['articulos_clave'][:4]
    relacionados = []
    if arts:
        ph = ','.join(['?']*len(arts))
        c.execute(f"""
            SELECT DISTINCT d.id,d.tipo,d.numero,d.anio,d.titulo,d.referencia,d.materia,d.paginas
            FROM articulos_idx ai JOIN documentos d ON d.id=ai.doc_id
            WHERE ai.articulo IN ({ph}) AND d.id!=?
            ORDER BY d.anio DESC LIMIT 8
        """, arts + [doc_id])
        relacionados = [dict(r) for r in c.fetchall()]

    # Historial de norma (mismos número/tipo de otros años)
    c.execute("""
        SELECT id,tipo,numero,anio,titulo,referencia,vigente
        FROM documentos WHERE tipo=? AND numero=? AND id!=?
        ORDER BY anio DESC
    """, (doc['tipo'], doc['numero'], doc_id))
    historial = [dict(r) for r in c.fetchall()]

    conn.close()
    return render_template('documento.html', doc=doc,
                           relacionados=relacionados, historial=historial)

@app.route('/api/cita/<int:doc_id>')
def cita(doc_id):
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT * FROM documentos WHERE id=?", (doc_id,))
    r = dict(c.fetchone()); conn.close()
    tf = tipo_formal(r['tipo'])
    resumen_txt = (r.get('resumen') or r.get('contenido') or '')[:400]
    fecha_fmt = r.get('fecha') or str(r.get('anio',''))
    mat = r.get('materia') or 'normativa tributaria'
    return jsonify({
        'cita_corta':  f"{tf} N°{r['numero']} de {r['anio']} del SII",
        'cita_media':  f"Conforme a lo señalado por el SII en {tf} N°{r['numero']} de {r['anio']}, en materia de {mat}",
        'cita_larga':  (
            f"En virtud de lo establecido en {tf} N°{r['numero']} de fecha {fecha_fmt}, "
            f"emanada del Servicio de Impuestos Internos, en materia de {mat}, "
            f"dicho organismo ha señalado que: \"{resumen_txt}...\""
        ),
        'cita_escrito': (
            f"En este contexto, cabe traer a colación lo señalado por el SII en su {tf} N°{r['numero']} "
            f"de {r['anio']} (ref. {r.get('referencia','')}), donde se establece que: "
            f"\"{resumen_txt}...\". "
            f"Dicha instrucción administrativa se encuentra disponible en {r.get('url_sii','www.sii.cl')}."
        ),
        'url': r.get('url_sii'), 'referencia': r.get('referencia')
    })

@app.route('/api/articulo')
def por_articulo():
    ley = request.args.get('ley','')
    art = request.args.get('art','')
    conn = get_db(); c = conn.cursor()
    c.execute("""
        SELECT DISTINCT d.id,d.tipo,d.numero,d.anio,d.titulo,d.referencia,d.materia,d.resumen
        FROM articulos_idx ai JOIN documentos d ON d.id=ai.doc_id
        WHERE (?='' OR ai.ley=?) AND (?='' OR ai.articulo LIKE ?)
        ORDER BY d.anio DESC LIMIT 100
    """, (ley,ley, art,f'%{art}%'))
    docs = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({'docs': docs, 'total': len(docs)})

@app.route('/api/stats')
def stats():
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM documentos"); total = c.fetchone()[0]
    c.execute("SELECT tipo,COUNT(*) cnt FROM documentos GROUP BY tipo")
    por_tipo = dict(c.fetchall())
    c.execute("SELECT anio,COUNT(*) cnt FROM documentos GROUP BY anio ORDER BY anio DESC LIMIT 10")
    por_anio = dict(c.fetchall())
    c.execute("SELECT MAX(fecha_carga) FROM documentos WHERE fuente='scraper'")
    ultima = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM scraper_log WHERE estado='ok' AND date(fecha)=date('now')")
    hoy = c.fetchone()[0]
    conn.close()
    return jsonify({'total':total,'por_tipo':por_tipo,'por_anio':por_anio,
                    'ultima_actualizacion':ultima,'nuevos_hoy':hoy})

# ── Panel Scraper ─────────────────────────────────────────────────────────
@app.route('/scraper')
def scraper_panel():
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT * FROM scraper_log ORDER BY fecha DESC LIMIT 100")
    logs = [dict(r) for r in c.fetchall()]
    c.execute("SELECT tipo,anio,COUNT(*) cnt,SUM(CASE WHEN estado='ok' THEN 1 ELSE 0 END) ok FROM scraper_log GROUP BY tipo,anio ORDER BY anio DESC LIMIT 30")
    resumen = [dict(r) for r in c.fetchall()]
    c.execute("SELECT value FROM scheduler_config WHERE key='ultima_ejecucion'")
    row = c.fetchone()
    ultima_ejecucion = row[0] if row else 'Nunca'
    conn.close()
    return render_template('scraper.html', logs=logs, resumen=resumen,
                           ultima_ejecucion=ultima_ejecucion,
                           status=scraper_status)

@app.route('/api/scraper/iniciar', methods=['POST'])
def iniciar_scraper():
    global scraper_status
    if scraper_status['running']:
        return jsonify({'ok': False, 'msg': 'Ya hay un scraper en ejecución'})

    data    = request.json or {}
    tipo    = data.get('tipo', 'circular')
    anio_d  = int(data.get('anio_desde', date.today().year))
    anio_h  = int(data.get('anio_hasta', date.today().year))
    delay   = float(data.get('delay', 1.0))

    with _lock:
        scraper_status.update({
            'running': True, 'messages': [], 'total': 0,
            'procesados': 0, 'nuevos': 0, 'errores': 0,
            'inicio': datetime.now().isoformat()
        })

    def run():
        try:
            import sys
            sys.path.insert(0, os.path.join(BASE, 'scraper'))
            from engine import scrape_anio

            for anio in range(anio_h, anio_d - 1, -1):
                push_msg(f"─── Iniciando {tipo.upper()} {anio} ───", True)
                result = scrape_anio(tipo, anio, callback=push_msg, delay=delay)
                push_msg(f"Año {anio} completado — {result.get('nuevos',0)} nuevos, {result.get('errores',0)} errores", True)

        except Exception as e:
            push_msg(f"ERROR CRÍTICO: {e}", False)
        finally:
            with _lock:
                scraper_status['running'] = False
            push_msg("SCRAPER_DONE", True)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return jsonify({'ok': True, 'msg': 'Scraper iniciado'})

@app.route('/api/scraper/historico', methods=['POST'])
def iniciar_historico():
    global scraper_status
    if scraper_status['running']:
        return jsonify({'ok': False, 'msg': 'Ya hay un scraper en ejecución'})

    data   = request.json or {}
    tipos  = data.get('tipos', ['circular'])
    desde  = int(data.get('desde', 2015))
    hasta  = int(data.get('hasta', date.today().year))
    delay  = float(data.get('delay', 1.5))

    with _lock:
        scraper_status.update({
            'running': True, 'messages': [], 'total': 0,
            'procesados': 0, 'nuevos': 0, 'errores': 0,
            'inicio': datetime.now().isoformat()
        })

    def run():
        try:
            import sys
            sys.path.insert(0, os.path.join(BASE, 'scraper'))
            from engine import scrape_anio

            for anio in range(hasta, desde - 1, -1):
                for tipo in tipos:
                    push_msg(f"─── {tipo.upper()} {anio} ───", True)
                    try:
                        result = scrape_anio(tipo, anio, callback=push_msg, delay=delay)
                        push_msg(f"✔ {tipo} {anio}: {result.get('nuevos',0)} nuevos", True)
                    except Exception as e:
                        push_msg(f"⚠ Error {tipo} {anio}: {e}", False)
                    import time; time.sleep(2)
        except Exception as e:
            push_msg(f"ERROR: {e}", False)
        finally:
            with _lock:
                scraper_status['running'] = False
            push_msg("SCRAPER_DONE", True)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({'ok': True})

@app.route('/api/scraper/detener', methods=['POST'])
def detener_scraper():
    with _lock:
        scraper_status['running'] = False
    push_msg("⏹ Scraper detenido por el usuario", False)
    return jsonify({'ok': True})

@app.route('/api/scraper/status')
def scraper_status_api():
    with _lock:
        msgs_nuevos = list(scraper_status['messages'])
        scraper_status['messages'] = []
    return jsonify({
        'running':    scraper_status['running'],
        'nuevos':     scraper_status['nuevos'],
        'errores':    scraper_status['errores'],
        'procesados': scraper_status['procesados'],
        'total':      scraper_status['total'],
        'messages':   msgs_nuevos,
    })

@app.route('/api/scraper/novedades', methods=['POST'])
def check_novedades():
    """Ejecuta verificación diaria manual"""
    if scraper_status['running']:
        return jsonify({'ok': False, 'msg': 'Scraper ocupado'})

    with _lock:
        scraper_status.update({'running': True, 'messages': [], 'nuevos': 0, 'errores': 0})

    def run():
        try:
            import sys
            sys.path.insert(0, os.path.join(BASE, 'scraper'))
            from engine import check_novedades as _check
            _check(callback=push_msg)
        except Exception as e:
            push_msg(f"Error: {e}", False)
        finally:
            with _lock:
                scraper_status['running'] = False
            push_msg("SCRAPER_DONE", True)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({'ok': True})

# ── Formulario agregar manual ─────────────────────────────────────────────
@app.route('/agregar', methods=['GET', 'POST'])
def agregar():
    if request.method == 'POST':
        d = request.json
        import sys; sys.path.insert(0, os.path.join(BASE,'scraper'))
        try:
            from engine import detectar_leyes, detectar_articulos
            texto = f"{d.get('titulo','')} {d.get('contenido','')}"
            if not d.get('leyes_citadas'):
                d['leyes_citadas'] = json.dumps(detectar_leyes(texto))
            if not d.get('articulos_clave'):
                d['articulos_clave'] = json.dumps(detectar_articulos(texto)[:20])
        except: pass

        d['hash_md5'] = hashlib.md5(
            (d.get('titulo','') + str(d.get('anio',''))).encode()
        ).hexdigest()
        if not d.get('referencia'):
            d['referencia'] = f"{tipo_formal(d['tipo'])} N°{d.get('numero','')} de {d.get('anio','')}"

        conn = get_db(); c = conn.cursor()
        try:
            c.execute("""INSERT OR IGNORE INTO documentos
                (hash_md5,tipo,numero,anio,fecha,titulo,materia,subtema,contenido,
                 resumen,url_sii,referencia,palabras_clave,leyes_citadas,articulos_clave)
                VALUES(:hash_md5,:tipo,:numero,:anio,:fecha,:titulo,:materia,:subtema,
                       :contenido,:resumen,:url_sii,:referencia,:palabras_clave,
                       :leyes_citadas,:articulos_clave)""", d)
            doc_id = c.lastrowid; conn.commit()
            return jsonify({'ok': True, 'id': doc_id})
        except Exception as e:
            return jsonify({'ok': False, 'error': str(e)})
        finally: conn.close()
    return render_template('agregar.html', materias=MATERIAS, leyes=LEYES)

# ── Scheduler diario ──────────────────────────────────────────────────────
def iniciar_scheduler():
    """Verifica novedades todos los días a las 08:00"""
    try:
        import schedule, time

        def job():
            if scraper_status['running']:
                log.info("Scheduler: scraper ocupado, saltando")
                return
            log.info("⏰ Scheduler: verificando novedades diarias")
            with _lock:
                scraper_status.update({'running': True, 'nuevos': 0, 'errores': 0})
            try:
                import sys
                sys.path.insert(0, os.path.join(BASE,'scraper'))
                from engine import check_novedades
                nuevos = check_novedades(callback=push_msg)
                log.info(f"✅ Scheduler completado — {nuevos} documentos nuevos")
                # Guardar timestamp
                conn = get_db()
                conn.execute("INSERT OR REPLACE INTO scheduler_config(key,value) VALUES('ultima_ejecucion',?)",
                             (datetime.now().isoformat(),))
                conn.commit(); conn.close()
            except Exception as e:
                log.error(f"Scheduler error: {e}")
            finally:
                with _lock: scraper_status['running'] = False

        schedule.every().day.at("08:00").do(job)

        def loop():
            while True:
                schedule.run_pending()
                time.sleep(60)

        t = threading.Thread(target=loop, daemon=True)
        t.start()
        log.info("✅ Scheduler diario activo — 08:00 AM")
        return t
    except ImportError:
        log.warning("'schedule' no instalado — scheduler no activo")
        return None

if __name__ == '__main__':
    init_db()
    scheduler_thread = iniciar_scheduler()
    log.info("\n" + "="*60)
    log.info("  SII Normativa V2.0")
    log.info("  http://localhost:5000")
    log.info("  Scheduler diario: 08:00 AM")
    log.info("="*60 + "\n")
    app.run(debug=False, port=5000, use_reloader=False)
