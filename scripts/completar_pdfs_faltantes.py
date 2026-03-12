import csv
import sqlite3
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from descargar_circulares_historicas import (
    BASE,
    construir_pdf_desde_texto,
    extraer_doc_con_antiword,
    extraer_doc_con_soffice,
    extraer_doc_con_word,
    limpiar_texto,
)

ROOT = Path(BASE)
DB_PATH = ROOT / 'data' / 'sii_normativa.db'
REPORT_CSV = ROOT / 'reports' / 'candidatos_conversion_a_pdf.csv'


def canonical_name(numero: str) -> str:
    raw = (numero or '').strip().lower().replace('º', '').replace('°', '')
    raw = raw.replace('n°', '').replace('nº', '').replace(' ', '')
    return f"circu{raw.zfill(2) if raw.isdigit() else raw}"


def read_candidates():
    with REPORT_CSV.open('r', encoding='utf-8-sig', newline='') as fh:
        return list(csv.DictReader(fh))


def fetch_doc(conn, doc_id: str):
    conn.row_factory = sqlite3.Row
    return conn.execute(
        "SELECT id, tipo, numero, anio, titulo, materia, contenido, url_sii FROM documentos WHERE id=?",
        (doc_id,),
    ).fetchone()


def convert_html_candidate(conn, row):
    doc = fetch_doc(conn, row['id_db'])
    if not doc:
        return 'missing_db'
    contenido = (doc['contenido'] or '').strip()
    if not contenido:
        return 'sin_contenido'
    nombre = canonical_name(doc['numero'])
    pdf_local = construir_pdf_desde_texto(contenido, doc['anio'], nombre, titulo='')
    return f'ok_html->{pdf_local}' if pdf_local else 'fail_html_pdf'


def convert_doc_candidate(conn, row):
    doc = fetch_doc(conn, row['id_db'])
    if not doc:
        return 'missing_db'
    ruta_doc = (row.get('ruta_doc') or '').strip()
    if not ruta_doc:
        return 'sin_ruta_doc'
    doc_path = ROOT / ruta_doc
    if not doc_path.exists():
        return 'doc_no_existe'
    texto = None
    for fn in (extraer_doc_con_antiword, extraer_doc_con_soffice, extraer_doc_con_word):
        texto = fn(str(doc_path))
        if texto:
            break
    if not texto:
        return 'doc_sin_texto'
    texto = limpiar_texto(texto)
    if len(texto) < 50:
        return 'doc_texto_corto'
    nombre = canonical_name(doc['numero'])
    pdf_local = construir_pdf_desde_texto(texto, doc['anio'], nombre, titulo=doc['titulo'] or '')
    return f'ok_doc->{pdf_local}' if pdf_local else 'fail_doc_pdf'


def main():
    rows = read_candidates()
    html_rows = [r for r in rows if r['estado_paridad'] == 'FALTA_PDF_PERO_HAY_HTML']
    doc_rows = [r for r in rows if r['estado_paridad'] == 'FALTA_PDF_PERO_HAY_DOC']

    conn = sqlite3.connect(DB_PATH)
    ok = 0
    fail = 0

    print('=' * 72)
    print('  COMPLETAR PDFS FALTANTES')
    print('=' * 72)
    print(f'HTML candidatos: {len(html_rows)}')
    print(f'DOC candidatos:  {len(doc_rows)}')

    for idx, row in enumerate(html_rows, 1):
        result = convert_html_candidate(conn, row)
        print(f"[HTML {idx}/{len(html_rows)}] {row['tipo']} {row['numero']}/{row['anio']} -> {result}")
        if result.startswith('ok_'):
            ok += 1
        else:
            fail += 1

    for idx, row in enumerate(doc_rows, 1):
        result = convert_doc_candidate(conn, row)
        print(f"[DOC  {idx}/{len(doc_rows)}] {row['tipo']} {row['numero']}/{row['anio']} -> {result}")
        if result.startswith('ok_'):
            ok += 1
        else:
            fail += 1

    conn.close()
    print('-' * 72)
    print(f'OK:    {ok}')
    print(f'FAIL:  {fail}')


if __name__ == '__main__':
    main()
