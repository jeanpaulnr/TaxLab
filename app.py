"""
TaxLab IA - Fase A sobre Flask + SQLite.
Mantiene la base documental y los endpoints legacy, y agrega la navegacion
/app, /admin, casos basicos y el endpoint inicial /api/chat.
"""

from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for
import sqlite3
import os
import json
import re
import hashlib
import threading
import logging
import textwrap
import unicodedata
from datetime import datetime, date

import fitz

from migraciones import run_migrations
from document_analysis import generate_document_analysis
from normativa_refs import CANONICAL_BODIES, build_article_ref, exact_article_refs, normalize_article_value, normalize_body_code, parse_article_ref_list
from rag import responder_consulta_tributaria
from pdf_layout import build_pdf_path

app = Flask(__name__)
BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, 'data', 'sii_normativa.db')

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('taxlab_app')

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
            'msg': msg,
            'ok': ok,
            'ts': datetime.now().isoformat(),
        })
        if total > 0:
            scraper_status['total'] = total
        if msg.startswith('OK') or msg.startswith('ok') or msg.startswith('Nuevo') or msg.startswith('✔') or msg.startswith('✅'):
            scraper_status['nuevos'] += 1
        if msg.startswith('WARN') or msg.startswith('Error') or msg.startswith('ERROR') or msg.startswith('⚠'):
            scraper_status['errores'] += 1
        scraper_status['procesados'] += 1
        if len(scraper_status['messages']) > 500:
            scraper_status['messages'] = scraper_status['messages'][-300:]


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
    pdf_local       TEXT,
    pdf_size_bytes  INTEGER DEFAULT 0,
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
    INSERT INTO docs_fts(rowid, titulo, materia, subtema, contenido, resumen, palabras_clave, leyes_citadas, articulos_clave)
    VALUES(new.id, new.titulo, new.materia, new.subtema, new.contenido, new.resumen, new.palabras_clave, new.leyes_citadas, new.articulos_clave);
END;
CREATE TRIGGER IF NOT EXISTS docs_ad AFTER DELETE ON documentos BEGIN
    INSERT INTO docs_fts(docs_fts, rowid, titulo, materia, subtema, contenido, resumen, palabras_clave, leyes_citadas, articulos_clave)
    VALUES('delete', old.id, old.titulo, old.materia, old.subtema, old.contenido, old.resumen, old.palabras_clave, old.leyes_citadas, old.articulos_clave);
END;
CREATE TRIGGER IF NOT EXISTS docs_au AFTER UPDATE ON documentos BEGIN
    INSERT INTO docs_fts(docs_fts, rowid, titulo, materia, subtema, contenido, resumen, palabras_clave, leyes_citadas, articulos_clave)
    VALUES('delete', old.id, old.titulo, old.materia, old.subtema, old.contenido, old.resumen, old.palabras_clave, old.leyes_citadas, old.articulos_clave);
    INSERT INTO docs_fts(rowid, titulo, materia, subtema, contenido, resumen, palabras_clave, leyes_citadas, articulos_clave)
    VALUES(new.id, new.titulo, new.materia, new.subtema, new.contenido, new.resumen, new.palabras_clave, new.leyes_citadas, new.articulos_clave);
END;
CREATE TABLE IF NOT EXISTS articulos_idx (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id   INTEGER REFERENCES documentos(id) ON DELETE CASCADE,
    ley      TEXT,
    articulo TEXT
);
CREATE INDEX IF NOT EXISTS idx_art_ley ON articulos_idx(ley, articulo);
CREATE UNIQUE INDEX IF NOT EXISTS idx_art_doc_ley_art ON articulos_idx(doc_id, ley, articulo);
CREATE INDEX IF NOT EXISTS idx_doc_tipo ON documentos(tipo, anio);
CREATE INDEX IF NOT EXISTS idx_doc_hash ON documentos(hash_md5);
CREATE INDEX IF NOT EXISTS idx_doc_fuente ON documentos(fuente);
CREATE TABLE IF NOT EXISTS document_analysis (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id                INTEGER NOT NULL UNIQUE REFERENCES documentos(id) ON DELETE CASCADE,
    status                TEXT DEFAULT 'pending',
    source_hash           TEXT,
    model                 TEXT,
    prompt_version        TEXT,
    summary_short         TEXT,
    summary_technical     TEXT,
    question_resolved     TEXT,
    holding_principal     TEXT,
    implicancia_practica  TEXT,
    normas_citadas_json   TEXT,
    articulos_citados_json TEXT,
    evidence_json         TEXT,
    confidence            TEXT,
    notes                 TEXT,
    generated_at          TEXT,
    created_at            TEXT DEFAULT (datetime('now')),
    updated_at            TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_document_analysis_status ON document_analysis(status);
CREATE TABLE IF NOT EXISTS judicial_docs (
    doc_id      INTEGER PRIMARY KEY REFERENCES documentos(id) ON DELETE CASCADE,
    sii_id      INTEGER UNIQUE,
    tipo_codigo TEXT,
    corte       TEXT,
    tribunal    TEXT,
    pdf_local   TEXT,
    html_local  TEXT
);
CREATE TABLE IF NOT EXISTS judicial_relaciones (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_id           INTEGER REFERENCES documentos(id) ON DELETE CASCADE,
    cuerpo_normativo TEXT,
    articulo         TEXT,
    nota             TEXT,
    UNIQUE(doc_id, cuerpo_normativo, articulo, nota)
);
CREATE INDEX IF NOT EXISTS idx_judicial_corte ON judicial_docs(corte);
CREATE INDEX IF NOT EXISTS idx_judicial_tribunal ON judicial_docs(tribunal);
CREATE INDEX IF NOT EXISTS idx_judicial_cuerpo ON judicial_relaciones(cuerpo_normativo);
CREATE INDEX IF NOT EXISTS idx_judicial_art ON judicial_relaciones(articulo);
CREATE TABLE IF NOT EXISTS historial (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    termino    TEXT,
    filtros    TEXT,
    fecha      TEXT DEFAULT (datetime('now')),
    resultados INTEGER
);
CREATE TABLE IF NOT EXISTS scraper_log (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo     TEXT,
    anio     INTEGER,
    numero   TEXT,
    estado   TEXT,
    url      TEXT,
    fecha    TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS scheduler_config (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""

MATERIAS_FALLBACK = []
LEYES_FALLBACK = list(CANONICAL_BODIES)


def get_asset_path(stored_path):
    if not stored_path:
        return None
    normalized = stored_path.replace('/', os.sep)
    return normalized if os.path.isabs(normalized) else os.path.join(BASE, normalized)


def estimar_paginas_texto(texto, chars_por_pagina=2800):
    limpio = (texto or '').strip()
    if not limpio:
        return 0
    return max(1, (len(limpio) + chars_por_pagina - 1) // chars_por_pagina)


def generar_pdf_desde_texto(titulo, texto):
    contenido = f"{(titulo or '').strip()}\n\n{(texto or '').strip()}".strip()
    if not contenido:
        return None

    doc = fitz.open()
    rect = fitz.paper_rect('a4')
    margin_x = 42
    margin_y = 48
    line_height = 14
    wrap_width = 95

    lineas = []
    for bloque in contenido.splitlines():
        bloque = bloque.rstrip()
        if not bloque:
            lineas.append('')
            continue
        partes = textwrap.wrap(
            bloque,
            width=wrap_width,
            replace_whitespace=False,
            drop_whitespace=False,
        ) or ['']
        lineas.extend([parte.rstrip() for parte in partes])

    pagina = None
    y = rect.height
    for linea in lineas:
        if pagina is None or y + line_height > rect.height - margin_y:
            pagina = doc.new_page(width=rect.width, height=rect.height)
            y = margin_y
        pagina.insert_text((margin_x, y), linea, fontsize=10.5, fontname='helv')
        y += line_height

    return doc.tobytes()


def build_manual_pdf(anio, tipo, numero, titulo, contenido):
    pdf_bytes = generar_pdf_desde_texto(titulo, contenido)
    if not pdf_bytes:
        return None, None, 0
    numero_seguro = re.sub(r'[^0-9A-Za-z._-]+', '_', str(numero or 'manual')).strip('._-') or 'manual'
    filename = f"{tipo}_{anio}_{numero_seguro}_manual.pdf"
    pdf_path = build_pdf_path(tipo, anio, filename)
    with open(pdf_path, 'wb') as handle:
        handle.write(pdf_bytes)
    return os.path.relpath(pdf_path, BASE).replace('\\', '/'), hashlib.md5(pdf_bytes).hexdigest(), len(pdf_bytes)


def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB), exist_ok=True)
    conn = get_db()
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()
    cambios = run_migrations(DB)
    if cambios:
        log.info('Migraciones aplicadas: %s', ', '.join(cambios))
    log.info('Base de datos inicializada')


def safe_json_loads(value, default=None):
    if default is None:
        default = []
    if not value:
        return list(default) if isinstance(default, list) else default
    try:
        return json.loads(value)
    except Exception:
        return list(default) if isinstance(default, list) else default


def normalize_law_codes(values):
    seen = set()
    normalized = []
    for value in values or []:
        code = normalize_body_code(value)
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return [code for code in CANONICAL_BODIES if code in seen]


def parse_exact_article_refs(value):
    return exact_article_refs(value)


def parse_ambiguous_article_refs(value):
    return [ref for ref in parse_article_ref_list(value) if ref.get('ambigua')]


def article_ref_chip(ref):
    if not ref:
        return ''
    if ref.get('ambigua'):
        articulo = ref.get('articulo') or ''
        return f'Art. {articulo} ambiguo'.strip()
    return ref.get('label') or ''


def build_article_match_value(articulo):
    return normalize_article_value(articulo)


def load_document_analysis(doc_id):
    conn = get_db()
    try:
        row = conn.execute(
            """
            SELECT *
            FROM document_analysis
            WHERE doc_id = ?
            """,
            (doc_id,),
        ).fetchone()
        if not row:
            return None
        analysis = dict(row)
        analysis['normas_citadas'] = normalize_law_codes(safe_json_loads(analysis.get('normas_citadas_json')))
        analysis['articulos_citados'] = parse_exact_article_refs(analysis.get('articulos_citados_json'))
        analysis['evidence'] = safe_json_loads(analysis.get('evidence_json'))
        return analysis
    finally:
        conn.close()


def tipo_formal(tipo):
    return {
        'circular': 'Circular',
        'oficio': 'Oficio',
        'resolucion': 'Resolucion Exenta',
        'judicial': 'Jurisprudencia Judicial',
    }.get(tipo, str(tipo).capitalize())


DISPLAY_REPLACEMENTS = (
    (r'\bley\s+sombre\b', 'Ley sobre'),
    (r'\bresoluciónes\b', 'Resoluciones'),
    (r'\bresoluciones ex\.\b', 'Resoluciones Ex.'),
)


def normalize_display_text(value):
    if not value:
        return ''
    text = str(value).replace('\r', '\n')
    for pattern, replacement in DISPLAY_REPLACEMENTS:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r' *\n *', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _text_fingerprint(value):
    text = normalize_display_text(value)
    text = ''.join(ch for ch in unicodedata.normalize('NFKD', text) if not unicodedata.combining(ch))
    text = text.lower()
    return re.sub(r'[^a-z0-9]+', '', text)


def _looks_like_heading(line):
    letters = [ch for ch in line if ch.isalpha()]
    if not letters:
        return False
    upper_ratio = sum(1 for ch in letters if ch.isupper()) / len(letters)
    return upper_ratio >= 0.82 and len(line.strip()) >= 24


def _is_redundant_line(line, *candidates):
    fp = _text_fingerprint(line)
    if not fp:
        return True
    if fp.startswith('home') or fp.startswith('resultadodestacado') or 'ingresountermino' in fp:
        return True
    for candidate in candidates:
        cfp = _text_fingerprint(candidate)
        if not cfp:
            continue
        if fp == cfp or fp in cfp or cfp in fp:
            return True
    return False


def build_display_subject(title, reference=''):
    text = normalize_display_text(title)
    if not text:
        return ''
    if _text_fingerprint(text) == _text_fingerprint(reference):
        return ''
    return text


def build_preview_text(content, title='', reference=''):
    text = normalize_display_text(content)
    if not text:
        return ''

    lines = [line.strip() for line in text.split('\n')]
    filtered = []
    skipping_header = True

    for line in lines:
        if not line:
            if filtered and filtered[-1] != '':
                filtered.append('')
            continue

        if skipping_header:
            if _is_redundant_line(line, title, reference):
                continue
            if re.search(r'^\(?(ord\.|oficio ordinario|resoluci[oó]n ex)', line, re.IGNORECASE):
                continue
            if _looks_like_heading(line):
                continue
            skipping_header = False

        filtered.append(line)

    preview = normalize_display_text('\n'.join(filtered))
    return preview or text


def build_display_summary(summary, content, title='', reference=''):
    candidate = normalize_display_text(summary)
    preview = build_preview_text(content, title=title, reference=reference)
    title_fp = _text_fingerprint(title)
    candidate_fp = _text_fingerprint(candidate)

    if title_fp and candidate_fp and candidate_fp.startswith(title_fp[:90]):
        candidate = preview

    if not candidate:
        candidate = preview
    if not candidate:
        return ''

    lines = [line.strip() for line in candidate.split('\n') if line.strip()]
    filtered = []
    for line in lines:
        if _is_redundant_line(line, title, reference):
            continue
        if re.search(r'^\(?(ord\.|oficio ordinario|resoluci[oó]n ex)', line, re.IGNORECASE):
            continue
        filtered.append(line)

    text = normalize_display_text(' '.join(filtered))
    return text or candidate



def get_total_docs():
    conn = get_db()
    try:
        return conn.execute('SELECT COUNT(*) FROM documentos').fetchone()[0]
    finally:
        conn.close()


def get_case_options(conn=None):
    owns_connection = conn is None
    conn = conn or get_db()
    try:
        rows = conn.execute("SELECT id, nombre, estado FROM casos ORDER BY fecha_modificacion DESC, id DESC").fetchall()
        return [dict(row) for row in rows]
    finally:
        if owns_connection:
            conn.close()


def get_catalog_laws(conn):
    leyes = set()

    try:
        rows = conn.execute(
            """
            SELECT DISTINCT ley
            FROM articulos_idx
            WHERE ley IS NOT NULL AND trim(ley) <> ''
            ORDER BY ley
            """
        ).fetchall()
        leyes.update(normalize_body_code(row[0]) for row in rows if row[0])
    except sqlite3.OperationalError:
        pass

    try:
        rows = conn.execute(
            """
            SELECT leyes_citadas
            FROM documentos
            WHERE leyes_citadas IS NOT NULL AND trim(leyes_citadas) <> ''
            """
        ).fetchall()
        for row in rows:
            leyes.update(normalize_body_code(value) for value in safe_json_loads(row[0]) if value)
    except sqlite3.OperationalError:
        pass

    leyes = {ley for ley in leyes if ley}
    ordered = [ley for ley in CANONICAL_BODIES if ley in leyes]
    return ordered or list(LEYES_FALLBACK)


def get_catalog_sources(conn):
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT fuente
            FROM documentos
            WHERE fuente IS NOT NULL AND trim(fuente) <> ''
            ORDER BY fuente
            """
        ).fetchall()
        return [row[0] for row in rows if row[0]]
    except sqlite3.OperationalError:
        return []


def get_catalog_context():
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM documentos')
        total = c.fetchone()[0]

        c.execute('SELECT tipo, COUNT(*) cnt FROM documentos GROUP BY tipo')
        por_tipo = {row['tipo']: row['cnt'] for row in c.fetchall()}

        c.execute('SELECT anio, COUNT(*) cnt FROM documentos WHERE anio IS NOT NULL GROUP BY anio ORDER BY anio DESC LIMIT 15')
        por_anio = [dict(row) for row in c.fetchall()]

        c.execute('SELECT termino, resultados FROM historial ORDER BY fecha DESC LIMIT 8')
        recientes = [dict(row) for row in c.fetchall()]

        c.execute(
            """
            SELECT id, tipo, numero, anio, fecha, fecha_carga, titulo, materia, resumen, referencia
            FROM documentos
            ORDER BY datetime(COALESCE(fecha_carga, fecha)) DESC, id DESC
            LIMIT 6
            """
        )
        docs_recientes = [dict(row) for row in c.fetchall()]

        c.execute(
            """
            SELECT ley, articulo, COUNT(*) cnt
            FROM articulos_idx
            WHERE ley IS NOT NULL AND articulo IS NOT NULL
            GROUP BY ley, articulo
            ORDER BY cnt DESC
            LIMIT 15
            """
        )
        top_arts = []
        for row in c.fetchall():
            ref = build_article_ref(row['ley'], row['articulo'])
            item = dict(row)
            item['clave'] = ref['clave']
            item['label'] = ref['label']
            item['slug'] = ref['slug']
            top_arts.append(item)

        c.execute("SELECT MAX(fecha_carga) FROM documentos")
        ultima_actualizacion = c.fetchone()[0]

        try:
            c.execute("SELECT DISTINCT corte FROM judicial_docs WHERE corte IS NOT NULL AND corte<>'' ORDER BY corte")
            cortes_judiciales = [row[0] for row in c.fetchall()]
            c.execute("SELECT DISTINCT cuerpo_normativo FROM judicial_relaciones WHERE cuerpo_normativo IS NOT NULL AND cuerpo_normativo<>'' ORDER BY cuerpo_normativo")
            cuerpos_judiciales = [row[0] for row in c.fetchall()]
        except sqlite3.OperationalError:
            cortes_judiciales, cuerpos_judiciales = [], []

        c.execute("SELECT DISTINCT materia FROM documentos WHERE materia IS NOT NULL AND trim(materia)<>'' ORDER BY materia")
        materias = [row[0] for row in c.fetchall()] or list(MATERIAS_FALLBACK)

        c.execute('SELECT COUNT(*) FROM casos')
        total_casos = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM scraper_log WHERE estado='ok' AND date(fecha)=date('now')")
        nuevos_hoy = c.fetchone()[0]

        return {
            'total': total,
            'por_tipo': por_tipo,
            'por_anio': por_anio,
            'recientes': recientes,
            'docs_recientes': docs_recientes,
            'top_arts': top_arts,
            'materias': materias,
            'leyes': get_catalog_laws(conn),
            'fuentes': get_catalog_sources(conn),
            'cortes_judiciales': cortes_judiciales,
            'cuerpos_judiciales': cuerpos_judiciales,
            'anio_actual': date.today().year,
            'ultima_actualizacion': ultima_actualizacion,
            'total_casos': total_casos,
            'nuevos_hoy': nuevos_hoy,
            'casos': get_case_options(conn),
        }
    finally:
        conn.close()


def render_app(template_name, *, active_section, page_title, page_subtitle, **context):
    shell_total_docs = context.get('total', get_total_docs())
    return render_template(
        template_name,
        active_section=active_section,
        page_title=page_title,
        page_subtitle=page_subtitle,
        shell_total_docs=shell_total_docs,
        **context,
    )

def load_document_context(doc_id):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute(
            """
            SELECT d.*, jd.sii_id, jd.tipo_codigo, jd.corte, jd.tribunal, jd.pdf_local AS judicial_pdf_local, jd.html_local
            FROM documentos d
            LEFT JOIN judicial_docs jd ON jd.doc_id = d.id
            WHERE d.id = ?
            """,
            (doc_id,),
        )
        row = c.fetchone()
        if not row:
            return None

        doc = dict(row)
        raw_article_refs = doc.get('articulos_clave')
        doc['leyes_citadas'] = normalize_law_codes(safe_json_loads(doc.get('leyes_citadas')))
        doc['articulos_clave'] = parse_exact_article_refs(raw_article_refs)
        doc['pdf_url'] = f'/documento/{doc_id}/pdf' if (doc.get('pdf_local') or doc.get('judicial_pdf_local')) else None
        doc['tema_mostrable'] = build_display_subject(doc.get('titulo'), doc.get('referencia'))
        doc['resumen_mostrable'] = build_display_summary(
            doc.get('resumen'),
            doc.get('contenido'),
            title=doc.get('titulo'),
            reference=doc.get('referencia'),
        )
        doc['preview_texto'] = build_preview_text(
            doc.get('contenido'),
            title=doc.get('titulo'),
            reference=doc.get('referencia'),
        )

        judicial_relaciones = []
        judicial_por_norma = []
        if doc['tipo'] == 'judicial':
            c.execute(
                """
                SELECT cuerpo_normativo, articulo, nota
                FROM judicial_relaciones
                WHERE doc_id = ?
                ORDER BY cuerpo_normativo, articulo, nota
                """,
                (doc_id,),
            )
            judicial_relaciones = [dict(r) for r in c.fetchall()]
            agrupado = {}
            for relacion in judicial_relaciones:
                cuerpo = relacion['cuerpo_normativo'] or 'Sin cuerpo normativo'
                valor = relacion['articulo'] or ''
                if relacion.get('nota'):
                    valor = f"{valor} ({relacion['nota']})" if valor else relacion['nota']
                agrupado.setdefault(cuerpo, []).append(valor or 'Sin articulo')
            judicial_por_norma = [
                {'cuerpo_normativo': cuerpo, 'articulos': articulos}
                for cuerpo, articulos in agrupado.items()
            ]

        relacionados = []
        articulos = doc['articulos_clave'][:4]
        if articulos:
            pair_clauses = ' OR '.join(['(ai.ley = ? AND ai.articulo = ?)'] * len(articulos))
            pair_params = []
            for ref in articulos:
                pair_params.extend([ref['cuerpo'], ref['articulo']])
            c.execute(
                f"""
                SELECT DISTINCT d.id, d.tipo, d.numero, d.anio, d.titulo, d.referencia, d.materia, d.paginas
                FROM articulos_idx ai
                JOIN documentos d ON d.id = ai.doc_id
                WHERE ({pair_clauses}) AND d.id != ?
                ORDER BY d.anio DESC
                LIMIT 8
                """,
                pair_params + [doc_id],
            )
            relacionados = [dict(row) for row in c.fetchall()]

        c.execute(
            """
            SELECT id, tipo, numero, anio, titulo, referencia, vigente
            FROM documentos
            WHERE tipo = ? AND numero = ? AND id != ?
            ORDER BY anio DESC
            """,
            (doc['tipo'], doc['numero'], doc_id),
        )
        historial = [dict(row) for row in c.fetchall()]

        return {
            'doc': doc,
            'relacionados': relacionados,
            'historial': historial,
            'judicial_relaciones': judicial_relaciones,
            'judicial_por_norma': judicial_por_norma,
            'analysis': load_document_analysis(doc_id),
        }
    finally:
        conn.close()


def load_scraper_context():
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute('SELECT * FROM scraper_log ORDER BY fecha DESC LIMIT 100')
        logs = [dict(row) for row in c.fetchall()]
        c.execute(
            """
            SELECT tipo, anio, COUNT(*) cnt,
                   SUM(CASE WHEN estado='ok' THEN 1 ELSE 0 END) ok
            FROM scraper_log
            GROUP BY tipo, anio
            ORDER BY anio DESC
            LIMIT 30
            """
        )
        resumen = [dict(row) for row in c.fetchall()]
        c.execute("SELECT value FROM scheduler_config WHERE key='ultima_ejecucion'")
        row = c.fetchone()
        ultima_ejecucion = row[0] if row else 'Nunca'
        return {
            'logs': logs,
            'resumen': resumen,
            'ultima_ejecucion': ultima_ejecucion,
            'status': scraper_status,
            'anio_actual': date.today().year,
        }
    finally:
        conn.close()


def get_case_overview():
    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT c.*, 
                   COUNT(n.id) AS total_notas,
                   SUM(CASE WHEN n.doc_id IS NOT NULL THEN 1 ELSE 0 END) AS total_docs
            FROM casos c
            LEFT JOIN caso_notas n ON n.caso_id = c.id
            GROUP BY c.id
            ORDER BY c.fecha_modificacion DESC, c.id DESC
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_case_detail(caso_id):
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute('SELECT * FROM casos WHERE id = ?', (caso_id,))
        caso = c.fetchone()
        if not caso:
            return None, []
        c.execute(
            """
            SELECT n.*, d.titulo AS doc_titulo, d.referencia AS doc_referencia
            FROM caso_notas n
            LEFT JOIN documentos d ON d.id = n.doc_id
            WHERE n.caso_id = ?
            ORDER BY n.fecha DESC, n.id DESC
            """,
            (caso_id,),
        )
        notas = [dict(row) for row in c.fetchall()]
        return dict(caso), notas
    finally:
        conn.close()


def get_doc_preview(doc_id):
    if not doc_id:
        return None
    conn = get_db()
    try:
        row = conn.execute(
            'SELECT id, titulo, referencia, tipo, anio FROM documentos WHERE id = ?',
            (doc_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


@app.route('/')
def index():
    context = get_catalog_context()
    return render_template('landing.html', **context)


@app.route('/app')
def app_dashboard():
    context = get_catalog_context()
    return render_app(
        'dashboard.html',
        active_section='dashboard',
        page_title='TaxLab IA',
        page_subtitle='Dashboard inicial para separar base publica SII, asistente con evidencia, casos privados, toolkit y paneles admin sin romper el producto actual.',
        **context,
    )


@app.route('/admin')
def admin_dashboard():
    catalog_context = get_catalog_context()
    scraper_context = load_scraper_context()
    merged_context = dict(catalog_context)
    merged_context.update(scraper_context)
    return render_app(
        'admin_dashboard.html',
        active_section='admin_dashboard',
        page_title='Admin Console',
        page_subtitle='Vista ejecutiva para controlar scraping, salud operativa e integridad del corpus sin mezclarla con la experiencia de usuario.',
        **merged_context,
    )


@app.route('/app/buscar')
def app_search():
    context = get_catalog_context()
    return render_app(
        'index.html',
        active_section='buscar',
        page_title='Base publica SII',
        page_subtitle='Busqueda documental sobre normativa tributaria y jurisprudencia judicial del SII, con filtros y trazabilidad por fuente.',
        **context,
    )


@app.route('/buscar')
def buscar():
    q = request.args.get('q', '').strip()
    tipo = request.args.get('tipo', '')
    anio = request.args.get('anio', '')
    ley = request.args.get('ley', '')
    materia = request.args.get('materia', '')
    articulo = request.args.get('articulo', '').strip()
    corte = request.args.get('corte', '').strip()
    cuerpo = request.args.get('cuerpo', '').strip()
    fuente = request.args.get('fuente', '').strip()
    vigente = request.args.get('vigente', '').strip()
    page = max(1, int(request.args.get('page', 1)))
    per = 15

    conn = get_db()
    try:
        c = conn.cursor()
        where = []
        params = []
        joins = 'LEFT JOIN judicial_docs jd ON jd.doc_id=d.id LEFT JOIN judicial_relaciones jr ON jr.doc_id=d.id'

        if q:
            try:
                fts_q = ' OR '.join(f'"{word}"' if len(word) > 3 else word for word in q.split())
                c.execute('SELECT rowid FROM docs_fts WHERE docs_fts MATCH ? ORDER BY rank LIMIT 500', (fts_q,))
                ids = [row[0] for row in c.fetchall()]
            except Exception:
                ids = []
            if ids:
                where.append(f"d.id IN ({','.join(['?'] * len(ids))})")
                params.extend(ids)
            else:
                like = f'%{q}%'
                where.append('(d.titulo LIKE ? OR d.contenido LIKE ? OR d.palabras_clave LIKE ? OR d.resumen LIKE ?)')
                params.extend([like] * 4)

        if tipo:
            where.append('d.tipo = ?')
            params.append(tipo)
        if anio:
            where.append('d.anio = ?')
            params.append(int(anio))
        if ley:
            ley = normalize_body_code(ley) or ley
            where.append('EXISTS (SELECT 1 FROM articulos_idx ai_ley WHERE ai_ley.doc_id = d.id AND ai_ley.ley = ?)')
            params.append(ley)
        if materia:
            where.append("COALESCE(d.materia, '') LIKE ?")
            params.append(f'%{materia}%')
        if articulo:
            articulo_match = build_article_match_value(articulo)
            if ley:
                where.append(
                    'EXISTS (SELECT 1 FROM articulos_idx ai_art WHERE ai_art.doc_id = d.id AND ai_art.ley = ? AND ai_art.articulo = ?)'
                )
                params.extend([ley, articulo_match])
            else:
                where.append('(d.contenido LIKE ? OR d.articulos_clave LIKE ?)')
                params.extend([f'%{articulo_match}%', f'%{articulo_match}%'])
        if corte:
            where.append('jd.corte = ?')
            params.append(corte)
        if cuerpo:
            where.append('jr.cuerpo_normativo = ?')
            params.append(cuerpo)
        if fuente:
            where.append('d.fuente = ?')
            params.append(fuente)
        if vigente in ('0', '1'):
            where.append('d.vigente = ?')
            params.append(int(vigente))

        clause = ('WHERE ' + ' AND '.join(where)) if where else ''
        base_from = f'FROM documentos d {joins} {clause}'

        c.execute(f'SELECT COUNT(*) FROM (SELECT d.id {base_from} GROUP BY d.id)', params)
        total = c.fetchone()[0]

        c.execute(
            f"""
            SELECT d.id, d.tipo, d.numero, d.anio, d.fecha, d.titulo, d.materia, d.fuente,
                   d.referencia, d.resumen, d.leyes_citadas, d.articulos_clave,
                   d.vigente, d.paginas, d.chars_texto, d.url_sii,
                   jd.corte, jd.tribunal, jd.tipo_codigo, COALESCE(d.pdf_local, jd.pdf_local) AS pdf_local,
                   GROUP_CONCAT(DISTINCT jr.cuerpo_normativo) AS cuerpos_normativos,
                   SUBSTR(d.contenido, 1, 2500) AS extracto
            {base_from}
            GROUP BY d.id
            ORDER BY COALESCE(d.fecha, '') DESC, d.id DESC
            LIMIT ? OFFSET ?
            """,
            params + [per, (page - 1) * per],
        )
        rows = c.fetchall()

        if q or tipo or ley or articulo or corte or cuerpo or vigente or materia or anio:
            try:
                c.execute(
                    'INSERT INTO historial(termino, filtros, resultados) VALUES(?,?,?)',
                    (
                        q,
                        json.dumps({
                            'tipo': tipo,
                            'anio': anio,
                            'ley': ley,
                            'materia': materia,
                            'articulo': articulo,
                            'corte': corte,
                            'cuerpo': cuerpo,
                            'fuente': fuente,
                            'vigente': vigente,
                        }),
                        total,
                    ),
                )
                conn.commit()
            except Exception:
                pass
    finally:
        conn.close()

    resultados = []
    for row in rows:
        tema = build_display_subject(row['titulo'], row['referencia'])
        extracto = build_display_summary(
            row['resumen'],
            row['extracto'],
            title=row['titulo'],
            reference=row['referencia'],
        )
        if q:
            for word in q.split():
                if len(word) > 2:
                    extracto = re.sub(f'(?i)({re.escape(word)})', r'<mark>\1</mark>', extracto)
        cuerpos_normativos = [value.strip() for value in (row['cuerpos_normativos'] or '').split(',') if value and value.strip()]
        resultados.append({
            'id': row['id'],
            'tipo': row['tipo'],
            'numero': row['numero'],
            'anio': row['anio'],
            'fecha': row['fecha'],
            'titulo': row['titulo'],
            'tema': tema,
            'materia': row['materia'],
            'referencia': row['referencia'],
            'extracto': extracto[:700] + ('...' if len(extracto) > 700 else ''),
            'leyes': normalize_law_codes(safe_json_loads(row['leyes_citadas'])),
            'articulos': parse_exact_article_refs(row['articulos_clave'])[:5],
            'vigente': row['vigente'],
            'paginas': row['paginas'] or 0,
            'url_sii': row['url_sii'],
            'corte': row['corte'],
            'tribunal': row['tribunal'],
            'tipo_codigo': row['tipo_codigo'],
            'cuerpos_normativos': cuerpos_normativos,
            'fuente': row['fuente'] if 'fuente' in row.keys() else '',
            'pdf_url': f"/documento/{row['id']}/pdf" if row['pdf_local'] else '',
        })

    return jsonify({
        'resultados': resultados,
        'total': total,
        'pagina': page,
        'paginas': max(1, (total + per - 1) // per),
    })

@app.route('/documento/<int:doc_id>')
def ver_documento_legacy(doc_id):
    return redirect(url_for('app_documento', doc_id=doc_id))


@app.route('/app/documento/<int:doc_id>')
def app_documento(doc_id):
    context = load_document_context(doc_id)
    if not context:
        return 'Documento no encontrado', 404
    catalog_context = get_catalog_context()
    return render_app(
        'documento.html',
        active_section='buscar',
        page_title='Detalle documental',
        page_subtitle='Ficha canónica del documento con citas, relacionados, fuente y opciones de guardado en casos.',
        total=catalog_context['total'],
        casos=catalog_context['casos'],
        anio_actual=catalog_context['anio_actual'],
        **context,
    )


@app.route('/api/documento/<int:doc_id>/analysis')
def get_document_analysis(doc_id):
    analysis = load_document_analysis(doc_id)
    if not analysis:
        return jsonify({'ok': False, 'analysis': None}), 404
    return jsonify({'ok': True, 'analysis': analysis})


@app.route('/api/documento/<int:doc_id>/analysis/generar', methods=['POST'])
def generate_analysis(doc_id):
    force = bool((request.get_json(silent=True) or {}).get('force'))
    try:
        analysis = generate_document_analysis(DB, doc_id, force=force)
    except Exception as exc:
        log.exception('Error generando document_analysis para %s', doc_id)
        return jsonify({'ok': False, 'error': str(exc)}), 500
    return jsonify({'ok': True, 'analysis': analysis})


@app.route('/documento/<int:doc_id>/pdf')
def descargar_pdf_documento(doc_id):
    conn = get_db()
    try:
        row = conn.execute(
            """
            SELECT d.tipo, d.referencia, COALESCE(d.pdf_local, jd.pdf_local) AS pdf_local
            FROM documentos d
            LEFT JOIN judicial_docs jd ON jd.doc_id = d.id
            WHERE d.id = ?
            """,
            (doc_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row or not row['pdf_local']:
        return 'PDF no disponible', 404

    abs_path = get_asset_path(row['pdf_local'])
    if not abs_path or not os.path.exists(abs_path):
        return 'PDF no encontrado en disco', 404

    return send_file(abs_path, as_attachment=True, download_name=os.path.basename(abs_path))


@app.route('/api/cita/<int:doc_id>')
def cita(doc_id):
    conn = get_db()
    try:
        row = conn.execute('SELECT * FROM documentos WHERE id = ?', (doc_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        return jsonify({'error': 'Documento no encontrado'}), 404

    doc = dict(row)
    tipo_nombre = tipo_formal(doc['tipo'])
    resumen_txt = (doc.get('resumen') or doc.get('contenido') or '')[:400]
    fecha_fmt = doc.get('fecha') or str(doc.get('anio', ''))
    materia = doc.get('materia') or 'normativa tributaria'

    if doc['tipo'] == 'judicial':
        base_ref = doc.get('referencia') or f"{tipo_nombre} {doc.get('numero', '')}"
        return jsonify({
            'cita_corta': base_ref,
            'cita_media': f'Conforme a lo resuelto en {base_ref}',
            'cita_larga': f'En {base_ref}, de fecha {fecha_fmt}, se sostuvo que: "{resumen_txt}..."',
            'cita_escrito': (
                f'En apoyo de lo expuesto, cabe citar {base_ref}, donde se razona que: "{resumen_txt}...". '
                f'El texto integro y su respaldo documental se encuentran disponibles en {doc.get("url_sii", "www.sii.cl")}. '
            ),
            'url': doc.get('url_sii'),
            'referencia': base_ref,
        })

    return jsonify({
        'cita_corta': f'{tipo_nombre} N{doc.get("numero", "")} de {doc.get("anio", "")} del SII',
        'cita_media': f'Conforme a lo señalado por el SII en {tipo_nombre} N{doc.get("numero", "")} de {doc.get("anio", "")}, en materia de {materia}',
        'cita_larga': (
            f'En virtud de lo establecido en {tipo_nombre} N{doc.get("numero", "")} de fecha {fecha_fmt}, '
            f'emanada del Servicio de Impuestos Internos, en materia de {materia}, dicho organismo ha señalado que: "{resumen_txt}..."'
        ),
        'cita_escrito': (
            f'En este contexto, cabe traer a colación lo señalado por el SII en su {tipo_nombre} N{doc.get("numero", "")} '
            f'de {doc.get("anio", "")} (ref. {doc.get("referencia", "")}), donde se establece que: "{resumen_txt}...". '
            f'Dicha instruccion administrativa se encuentra disponible en {doc.get("url_sii", "www.sii.cl")}. '
        ),
        'url': doc.get('url_sii'),
        'referencia': doc.get('referencia'),
    })


@app.route('/api/articulo')
def por_articulo():
    ley = normalize_body_code(request.args.get('ley', '')) or ''
    articulo = build_article_match_value(request.args.get('art', ''))
    conn = get_db()
    try:
        if not ley or not articulo:
            docs = []
        else:
            rows = conn.execute(
                """
                SELECT DISTINCT d.id, d.tipo, d.numero, d.anio, d.titulo, d.referencia, d.materia, d.resumen
                FROM articulos_idx ai
                JOIN documentos d ON d.id = ai.doc_id
                WHERE ai.ley = ? AND ai.articulo = ?
                ORDER BY d.anio DESC
                LIMIT 100
                """,
                (ley, articulo),
            ).fetchall()
            docs = [dict(row) for row in rows]
    finally:
        conn.close()
    return jsonify({'docs': docs, 'total': len(docs)})


@app.route('/api/stats')
def stats():
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM documentos')
        total = c.fetchone()[0]
        c.execute('SELECT tipo, COUNT(*) cnt FROM documentos GROUP BY tipo')
        por_tipo = {row[0]: row[1] for row in c.fetchall()}
        c.execute('SELECT anio, COUNT(*) cnt FROM documentos GROUP BY anio ORDER BY anio DESC LIMIT 10')
        por_anio = {row[0]: row[1] for row in c.fetchall()}
        c.execute('SELECT MAX(fecha_carga) FROM documentos')
        ultima = c.fetchone()[0]
        c.execute("SELECT COUNT(*) FROM scraper_log WHERE estado='ok' AND date(fecha)=date('now')")
        hoy = c.fetchone()[0]
    finally:
        conn.close()

    return jsonify({
        'total': total,
        'por_tipo': por_tipo,
        'por_anio': por_anio,
        'ultima_actualizacion': ultima,
        'nuevos_hoy': hoy,
    })


@app.route('/app/asistente')
def app_asistente():
    context = get_catalog_context()
    return render_app(
        'asistente.html',
        active_section='asistente',
        page_title='Asistente tributario',
        page_subtitle='Interfaz inicial para RAG con evidencia. Responde solo sobre el corpus disponible y siempre deja fuentes trazables.',
        **context,
    )


@app.route('/api/chat', methods=['POST'])
def chat_tributario():
    data = request.get_json(silent=True) or {}
    pregunta = (data.get('pregunta') or '').strip()
    filtros = data.get('filtros') or {}
    if not pregunta:
        return jsonify({'error': 'Debes enviar una pregunta.'}), 400
    try:
        respuesta = responder_consulta_tributaria(DB, pregunta, filtros=filtros)
        return jsonify(respuesta)
    except Exception as exc:
        log.exception('Error en /api/chat')
        return jsonify({'error': f'No fue posible responder la consulta: {exc}'}), 500


@app.route('/api/casos')
def api_casos():
    return jsonify({'casos': get_case_options()})


@app.route('/app/casos', methods=['GET', 'POST'])
def app_casos():
    if request.method == 'POST':
        nombre = (request.form.get('nombre') or '').strip()
        rut_cliente = (request.form.get('rut_cliente') or '').strip() or None
        descripcion = (request.form.get('descripcion') or '').strip() or None
        estado = (request.form.get('estado') or 'activo').strip() or 'activo'
        if nombre:
            conn = get_db()
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO casos(nombre, rut_cliente, descripcion, estado, fecha_creacion, fecha_modificacion)
                    VALUES(?, ?, ?, ?, datetime('now'), datetime('now'))
                    """,
                    (nombre, rut_cliente, descripcion, estado),
                )
                conn.commit()
                case_id = cursor.lastrowid
            finally:
                conn.close()
            return redirect(url_for('app_casos', caso=case_id))

    catalog_context = get_catalog_context()
    casos = get_case_overview()
    selected_case_id = request.args.get('caso', type=int)
    if not selected_case_id and casos:
        selected_case_id = casos[0]['id']
    caso_activo, notas_caso = get_case_detail(selected_case_id) if selected_case_id else (None, [])
    prefill_doc = get_doc_preview(request.args.get('doc_id', type=int))
    return render_app(
        'casos.html',
        active_section='casos',
        page_title='Casos y vaults privados',
        page_subtitle='CRUD minimo de carpetas por cliente o asunto para guardar notas, consultas IA y documentos relevantes.',
        total=catalog_context['total'],
        casos=casos,
        caso_activo=caso_activo,
        notas_caso=notas_caso,
        prefill_doc=prefill_doc,
    )


@app.route('/app/casos/<int:caso_id>/notas', methods=['POST'])
def agregar_nota_caso(caso_id):
    contenido = (request.form.get('contenido') or '').strip() or None
    tipo = (request.form.get('tipo') or 'nota').strip()
    doc_id = request.form.get('doc_id', type=int)
    conn = get_db()
    try:
        caso = conn.execute('SELECT id FROM casos WHERE id = ?', (caso_id,)).fetchone()
        if not caso:
            return 'Caso no encontrado', 404
        conn.execute("UPDATE casos SET fecha_modificacion=datetime('now') WHERE id=?", (caso_id,))
        conn.execute(
            "INSERT INTO caso_notas(caso_id, contenido, tipo, doc_id, fecha) VALUES(?, ?, ?, ?, datetime('now'))",
            (caso_id, contenido, tipo, doc_id),
        )
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for('app_casos', caso=caso_id))

@app.route('/api/casos/<int:caso_id>/guardar-documento', methods=['POST'])
def guardar_documento_en_caso(caso_id):
    data = request.get_json(silent=True) or {}
    doc_id = data.get('doc_id')
    comentario = (data.get('comentario') or '').strip() or None
    if not doc_id:
        return jsonify({'ok': False, 'error': 'Debes indicar el documento.'}), 400

    conn = get_db()
    try:
        caso = conn.execute('SELECT id FROM casos WHERE id = ?', (caso_id,)).fetchone()
        doc = conn.execute('SELECT referencia, titulo FROM documentos WHERE id = ?', (doc_id,)).fetchone()
        if not caso:
            return jsonify({'ok': False, 'error': 'Caso no encontrado.'}), 404
        if not doc:
            return jsonify({'ok': False, 'error': 'Documento no encontrado.'}), 404

        contenido = comentario or f"Documento vinculado: {doc['referencia'] or doc['titulo']}"
        conn.execute("UPDATE casos SET fecha_modificacion=datetime('now') WHERE id=?", (caso_id,))
        conn.execute(
            """
            INSERT INTO caso_notas(caso_id, contenido, tipo, doc_id, fecha)
            VALUES(?, ?, 'documento_adjunto', ?, datetime('now'))
            """,
            (caso_id, contenido, doc_id),
        )
        conn.commit()
    finally:
        conn.close()

    return jsonify({'ok': True})


@app.route('/api/casos/<int:caso_id>/guardar-consulta', methods=['POST'])
def guardar_consulta_en_caso(caso_id):
    data = request.get_json(silent=True) or {}
    pregunta = (data.get('pregunta') or '').strip()
    respuesta = (data.get('respuesta') or '').strip()
    fundamento = (data.get('fundamento') or '').strip()
    riesgos = (data.get('riesgos') or '').strip()
    confianza = (data.get('confianza') or '').strip()
    nota = (data.get('nota') or '').strip()
    fuentes = data.get('fuentes') or []

    if not pregunta and not respuesta:
        return jsonify({'ok': False, 'error': 'No hay contenido para guardar.'}), 400

    fuentes_texto = '\n'.join(
        f"- {fuente.get('referencia', 'Documento')} (ID {fuente.get('id')})"
        for fuente in fuentes
    ) or '- Sin fuentes asociadas'
    contenido = (
        f"Pregunta:\n{pregunta}\n\n"
        f"Respuesta:\n{respuesta}\n\n"
        f"Fundamento:\n{fundamento}\n\n"
        f"Riesgos:\n{riesgos}\n\n"
        f"Confianza:\n{confianza}\n\n"
        f"Nota:\n{nota}\n\n"
        f"Fuentes:\n{fuentes_texto}"
    )

    conn = get_db()
    try:
        caso = conn.execute('SELECT id FROM casos WHERE id = ?', (caso_id,)).fetchone()
        if not caso:
            return jsonify({'ok': False, 'error': 'Caso no encontrado.'}), 404
        conn.execute("UPDATE casos SET fecha_modificacion=datetime('now') WHERE id=?", (caso_id,))
        conn.execute(
            """
            INSERT INTO caso_notas(caso_id, contenido, tipo, doc_id, fecha)
            VALUES(?, ?, 'consulta_ia', NULL, datetime('now'))
            """,
            (caso_id, contenido),
        )
        conn.commit()
    finally:
        conn.close()

    return jsonify({'ok': True})


@app.route('/app/toolkit')
def app_toolkit():
    context = get_catalog_context()
    return render_app(
        'toolkit.html',
        active_section='toolkit',
        page_title='Toolkit tributario',
        page_subtitle='Primer espacio para calculadoras, calendario y tablas auxiliares sin mezclarlo con el corpus documental.',
        **context,
    )


@app.route('/scraper')
def scraper_panel_legacy():
    return redirect(url_for('admin_scraper'))


@app.route('/admin/scraper')
def admin_scraper():
    context = load_scraper_context()
    total = get_total_docs()
    return render_app(
        'scraper.html',
        active_section='admin_scraper',
        page_title='Panel de scraping',
        page_subtitle='Control operativo del corpus, monitoreo del scheduler y lanzamientos manuales del scraper existente.',
        total=total,
        **context,
    )


@app.route('/admin/ingestion')
def admin_ingestion():
    context = get_catalog_context()
    return render_app(
        'admin_ingestion.html',
        active_section='admin_ingestion',
        page_title='Ingestion y QA',
        page_subtitle='Stub inicial para futuras fuentes, chequeos de completitud y flujos de enriquecimiento documental.',
        **context,
    )


@app.route('/api/scraper/iniciar', methods=['POST'])
def iniciar_scraper():
    global scraper_status
    if scraper_status['running']:
        return jsonify({'ok': False, 'msg': 'Ya hay un scraper en ejecucion'})

    data = request.json or {}
    tipo = data.get('tipo', 'circular')
    anio_desde = int(data.get('anio_desde', date.today().year))
    anio_hasta = int(data.get('anio_hasta', date.today().year))
    delay = float(data.get('delay', 1.0))

    with _lock:
        scraper_status.update({
            'running': True,
            'messages': [],
            'total': 0,
            'procesados': 0,
            'nuevos': 0,
            'errores': 0,
            'inicio': datetime.now().isoformat(),
        })

    def run():
        try:
            import sys
            sys.path.insert(0, os.path.join(BASE, 'scraper'))
            from engine import scrape_anio

            for anio in range(anio_hasta, anio_desde - 1, -1):
                push_msg(f'--- Iniciando {tipo.upper()} {anio} ---', True)
                result = scrape_anio(tipo, anio, callback=push_msg, delay=delay)
            push_msg(f'Año {anio} completado - {result.get("nuevos", 0)} nuevos, {result.get("errores", 0)} errores', True)
        except Exception as exc:
            push_msg(f'ERROR CRITICO: {exc}', False)
        finally:
            with _lock:
                scraper_status['running'] = False
            push_msg('SCRAPER_DONE', True)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({'ok': True, 'msg': 'Scraper iniciado'})


@app.route('/api/scraper/historico', methods=['POST'])
def iniciar_historico():
    global scraper_status
    if scraper_status['running']:
        return jsonify({'ok': False, 'msg': 'Ya hay un scraper en ejecucion'})

    data = request.json or {}
    tipos = data.get('tipos', ['circular'])
    desde = int(data.get('desde', 2015))
    hasta = int(data.get('hasta', date.today().year))
    delay = float(data.get('delay', 1.5))

    with _lock:
        scraper_status.update({
            'running': True,
            'messages': [],
            'total': 0,
            'procesados': 0,
            'nuevos': 0,
            'errores': 0,
            'inicio': datetime.now().isoformat(),
        })

    def run():
        try:
            import sys
            import time
            sys.path.insert(0, os.path.join(BASE, 'scraper'))
            from engine import scrape_anio

            for anio in range(hasta, desde - 1, -1):
                for tipo in tipos:
                    push_msg(f'--- {tipo.upper()} {anio} ---', True)
                    try:
                        result = scrape_anio(tipo, anio, callback=push_msg, delay=delay)
                        push_msg(f'OK {tipo} {anio}: {result.get("nuevos", 0)} nuevos', True)
                    except Exception as exc:
                        push_msg(f'WARN {tipo} {anio}: {exc}', False)
                    time.sleep(2)
        except Exception as exc:
            push_msg(f'ERROR: {exc}', False)
        finally:
            with _lock:
                scraper_status['running'] = False
            push_msg('SCRAPER_DONE', True)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({'ok': True})

@app.route('/api/scraper/detener', methods=['POST'])
def detener_scraper():
    with _lock:
        scraper_status['running'] = False
    push_msg('Scraper detenido por el usuario', False)
    return jsonify({'ok': True})


@app.route('/api/scraper/status')
def scraper_status_api():
    with _lock:
        nuevos = list(scraper_status['messages'])
        scraper_status['messages'] = []
        return jsonify({
            'running': scraper_status['running'],
            'nuevos': scraper_status['nuevos'],
            'errores': scraper_status['errores'],
            'procesados': scraper_status['procesados'],
            'total': scraper_status['total'],
            'messages': nuevos,
        })


@app.route('/api/scraper/novedades', methods=['POST'])
def check_novedades():
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
        except Exception as exc:
            push_msg(f'Error: {exc}', False)
        finally:
            with _lock:
                scraper_status['running'] = False
            push_msg('SCRAPER_DONE', True)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({'ok': True})


@app.route('/agregar', methods=['GET', 'POST'])
def agregar():
    if request.method == 'POST':
        data = request.json or {}
        import sys
        sys.path.insert(0, os.path.join(BASE, 'scraper'))
        try:
            from engine import detectar_leyes, detectar_articulos, guardar_documento
            texto = f"{data.get('titulo', '')} {data.get('contenido', '')}"
            if not data.get('leyes_citadas'):
                data['leyes_citadas'] = json.dumps(detectar_leyes(texto))
            if not data.get('articulos_clave'):
                data['articulos_clave'] = json.dumps(detectar_articulos(texto)[:20])
        except Exception:
            pass

        contenido = (data.get('contenido') or '').strip()
        anio = int(data.get('anio') or date.today().year)
        tipo = (data.get('tipo') or 'manual').strip().lower()
        numero = (data.get('numero') or '').strip()
        pdf_local, pdf_hash, pdf_size = build_manual_pdf(anio, tipo, numero, data.get('titulo', ''), contenido)
        if not pdf_local:
            return jsonify({'ok': False, 'error': 'No fue posible generar el PDF canónico del documento manual'})

        data['hash_md5'] = pdf_hash
        data['anio'] = anio
        data['tipo'] = tipo
        data['paginas'] = estimar_paginas_texto(contenido)
        data['chars_texto'] = len(contenido)
        data['pdf_local'] = pdf_local
        data['pdf_size_bytes'] = pdf_size
        data['fuente'] = data.get('fuente') or 'manual'
        if not data.get('referencia'):
            data['referencia'] = f"{tipo_formal(data['tipo'])} N{data.get('numero', '')} de {data.get('anio', '')}"

        try:
            doc_id = guardar_documento(data)
            if not doc_id:
                return jsonify({'ok': False, 'error': 'No fue posible guardar el documento manual'})
            return jsonify({'ok': True, 'id': doc_id})
        except Exception as exc:
            return jsonify({'ok': False, 'error': str(exc)})

    context = get_catalog_context()
    return render_app(
        'agregar.html',
        active_section='agregar',
        page_title='Agregar documento manual',
        page_subtitle='Carga puntual para documentos fuera del flujo automatico o para apoyo editorial del corpus.',
        **context,
    )


def iniciar_scheduler():
    try:
        import schedule
        import time

        def job():
            if scraper_status['running']:
                log.info('Scheduler: scraper ocupado, se omite esta corrida')
                return
            log.info('Scheduler: verificando novedades diarias')
            with _lock:
                scraper_status.update({'running': True, 'nuevos': 0, 'errores': 0})
            try:
                import sys
                sys.path.insert(0, os.path.join(BASE, 'scraper'))
                from engine import check_novedades
                nuevos = check_novedades(callback=push_msg)
                log.info('Scheduler completado - %s documentos nuevos', nuevos)
                conn = get_db()
                conn.execute(
                    "INSERT OR REPLACE INTO scheduler_config(key, value) VALUES('ultima_ejecucion', ?)",
                    (datetime.now().isoformat(),),
                )
                conn.commit()
                conn.close()
            except Exception as exc:
                log.error('Scheduler error: %s', exc)
            finally:
                with _lock:
                    scraper_status['running'] = False

        schedule.every().day.at('08:00').do(job)

        def loop():
            while True:
                schedule.run_pending()
                time.sleep(60)

        thread = threading.Thread(target=loop, daemon=True)
        thread.start()
        log.info('Scheduler diario activo - 08:00 AM')
        return thread
    except ImportError:
        log.warning("'schedule' no instalado - scheduler no activo")
        return None


if __name__ == '__main__':
    init_db()
    scheduler_thread = iniciar_scheduler()
    log.info('\n' + '=' * 60)
    log.info('  TaxLab IA - Fase A')
    log.info('  http://localhost:5000')
    log.info('  Scheduler diario: 08:00 AM')
    log.info('=' * 60 + '\n')
    app.run(debug=False, port=5000, use_reloader=False)

