import csv
import shutil
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / 'data' / 'sii_normativa.db'
HuerfanosCSV = ROOT / 'reports' / 'paridad_pdfs_huerfanos.csv'
QUARANTINE = ROOT / 'orphan_pdfs'

JUDICIAL_DOC_ID = 8857
JUDICIAL_PDF = Path('pdfs/judicial/2015/judicial_2015_RIT_GR-18-00613-2013_id1112.pdf')
JUDICIAL_HTML = Path('pdfs/judicial_html/2015/judicial_2015_RIT_GR-18-00613-2013_id1112.html')


def reconcile_judicial(conn):
    pdf_abs = ROOT / JUDICIAL_PDF
    html_abs = ROOT / JUDICIAL_HTML
    if not pdf_abs.exists():
        return 'judicial_pdf_no_existe'
    conn.execute(
        "UPDATE judicial_docs SET pdf_local=?, html_local=?, sii_id=COALESCE(sii_id, 1112) WHERE doc_id=?",
        (str(JUDICIAL_PDF).replace('/', '\\'), str(JUDICIAL_HTML).replace('/', '\\') if html_abs.exists() else None, JUDICIAL_DOC_ID),
    )
    return 'judicial_ok'


def move_resolution_orphans():
    moved = []
    if not HuerfanosCSV.exists():
        return moved
    QUARANTINE.mkdir(exist_ok=True)
    with HuerfanosCSV.open('r', encoding='utf-8-sig', newline='') as fh:
        rows = list(csv.DictReader(fh))
    for row in rows:
        rel = (row.get('ruta_pdf') or '').strip()
        if not rel.startswith('pdfs\\resolucion_'):
            continue
        src = ROOT / rel
        if not src.exists():
            continue
        dst = QUARANTINE / src.name
        if dst.exists():
            dst = QUARANTINE / f"dup_{src.name}"
        shutil.move(str(src), str(dst))
        moved.append((safe_rel(src), safe_rel(dst)))
    return moved


def safe_rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def main():
    conn = sqlite3.connect(DB_PATH)
    result = reconcile_judicial(conn)
    conn.commit()
    conn.close()
    moved = move_resolution_orphans()
    print('judicial:', result)
    print('moved_resolutions:', len(moved))
    for src, dst in moved:
        print(src, '->', dst)


if __name__ == '__main__':
    main()
