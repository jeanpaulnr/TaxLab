import argparse
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / 'data'
DB_PATH = DATA_DIR / 'sii_normativa.db'
BACKUP_DIR = DATA_DIR / 'backups'
PDF_ROOT = ROOT / 'pdfs'
HTML_ROOT = ROOT / 'html_historico'
IMG_ROOT = ROOT / 'img_historico'
OCR_ROOT = ROOT / 'ocr_historico'
DOC_ROOT = ROOT / 'doc_historico'
TARGET_TYPES = ('circular', 'oficio', 'resolucion')
LOG_TYPES = ('circular', 'resolucion', 'oficio_iva', 'oficio_lir', 'oficio_otras', 'circular_historica')

sys.path.insert(0, str(ROOT))
from app import init_db


def get_conn(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def delete_year_dir(base: Path, year: int) -> int:
    path = base / str(year)
    if not path.exists():
        return 0
    count = sum(1 for p in path.rglob('*') if p.is_file())
    shutil.rmtree(path)
    return count


def table_columns(conn, db_alias, table):
    return [row['name'] for row in conn.execute(f"PRAGMA {db_alias}.table_info({table})").fetchall()]


def copy_table(conn, table, where_sql=None, params=()):
    src_cols = table_columns(conn, 'old', table)
    dst_cols = table_columns(conn, 'main', table)
    cols = [col for col in src_cols if col in dst_cols]
    if not cols:
        return 0
    col_list = ', '.join(cols)
    sql = f"INSERT INTO {table} ({col_list}) SELECT {col_list} FROM old.{table}"
    if where_sql:
        sql += ' WHERE ' + where_sql
    before = conn.total_changes
    conn.execute(sql, params)
    return conn.total_changes - before


def clean_target_files(desde, hasta):
    files_deleted = 0
    for year in range(desde, hasta + 1):
        files_deleted += delete_year_dir(PDF_ROOT / 'circular', year)
        files_deleted += delete_year_dir(PDF_ROOT / 'oficio', year)
        files_deleted += delete_year_dir(PDF_ROOT / 'resolucion', year)
        if year <= 2012:
            files_deleted += delete_year_dir(HTML_ROOT, year)
            files_deleted += delete_year_dir(IMG_ROOT, year)
            files_deleted += delete_year_dir(OCR_ROOT, year)
            files_deleted += delete_year_dir(DOC_ROOT, year)
    return files_deleted


def main():
    parser = argparse.ArgumentParser(description='Recrear base preservando lo no objetivo y limpiando 1995-2026 no judicial')
    parser.add_argument('--desde', type=int, default=1995)
    parser.add_argument('--hasta', type=int, default=2026)
    parser.add_argument('--yes', action='store_true')
    args = parser.parse_args()

    if not DB_PATH.exists():
        raise SystemExit(f'No existe la base: {DB_PATH}')

    with get_conn(DB_PATH) as conn:
        target_docs = conn.execute(
            '''SELECT COUNT(*) FROM documentos
               WHERE anio BETWEEN ? AND ?
                 AND tipo IN ('circular','oficio','resolucion')''',
            (args.desde, args.hasta),
        ).fetchone()[0]
        preserved_docs = conn.execute(
            '''SELECT COUNT(*) FROM documentos
               WHERE NOT (anio BETWEEN ? AND ? AND tipo IN ('circular','oficio','resolucion'))''',
            (args.desde, args.hasta),
        ).fetchone()[0]

    print('=' * 72)
    print('  RECREACION DE BASE PRESERVANDO FUERA DEL OBJETIVO')
    print('=' * 72)
    print(f'Base: {DB_PATH}')
    print(f'Rango a rehacer: {args.desde}-{args.hasta}')
    print(f'Documentos objetivo a eliminar: {target_docs}')
    print(f'Documentos a preservar:         {preserved_docs}')
    print('Se preserva todo lo que NO sea circular/oficio/resolucion dentro del rango.')
    print('Tambien se limpiaran PDFs y auxiliares del bloque objetivo.')

    if not args.yes:
        print('-' * 72)
        print('Dry run. Agrega --yes para ejecutar.')
        return

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = BACKUP_DIR / f'sii_normativa_pre_rebuild_{stamp}.db'
    shutil.copy2(DB_PATH, backup_path)

    for suffix in ('', '-wal', '-shm'):
        candidate = Path(str(DB_PATH) + suffix)
        if candidate.exists():
            candidate.unlink()

    init_db()

    conn = get_conn(DB_PATH)
    try:
        conn.execute('PRAGMA foreign_keys=OFF')
        conn.execute('BEGIN')
        conn.execute(f"ATTACH DATABASE '{backup_path.as_posix()}' AS old")

        docs_copied = copy_table(
            conn,
            'documentos',
            "NOT (anio BETWEEN ? AND ? AND tipo IN ('circular','oficio','resolucion'))",
            (args.desde, args.hasta),
        )
        articulos_copied = copy_table(
            conn,
            'articulos_idx',
            'doc_id IN (SELECT id FROM main.documentos)',
        )
        judicial_docs_copied = copy_table(
            conn,
            'judicial_docs',
            'doc_id IN (SELECT id FROM main.documentos)',
        )
        judicial_rel_copied = copy_table(
            conn,
            'judicial_relaciones',
            'doc_id IN (SELECT id FROM main.documentos)',
        )
        historial_copied = copy_table(conn, 'historial')
        scheduler_copied = copy_table(conn, 'scheduler_config')
        casos_copied = copy_table(conn, 'casos') if 'casos' in [r['name'] for r in conn.execute("SELECT name FROM old.sqlite_master WHERE type='table'").fetchall()] else 0
        caso_notas_copied = copy_table(
            conn,
            'caso_notas',
            '(doc_id IS NULL OR doc_id IN (SELECT id FROM main.documentos))',
        ) if 'caso_notas' in [r['name'] for r in conn.execute("SELECT name FROM old.sqlite_master WHERE type='table'").fetchall()] else 0
        scraper_log_copied = copy_table(
            conn,
            'scraper_log',
            f"NOT (anio BETWEEN ? AND ? AND tipo IN ({','.join('?' for _ in LOG_TYPES)}))",
            (args.desde, args.hasta, *LOG_TYPES),
        )

        try:
            conn.execute("INSERT INTO docs_fts(docs_fts) VALUES('rebuild')")
        except Exception:
            pass
        conn.execute('DETACH DATABASE old')
        conn.commit()
    finally:
        conn.close()

    files_deleted = clean_target_files(args.desde, args.hasta)

    print('-' * 72)
    print(f'backup_db:              {backup_path}')
    print(f'docs_preservados:       {docs_copied}')
    print(f'articulos_preservados:  {articulos_copied}')
    print(f'judicial_docs:          {judicial_docs_copied}')
    print(f'judicial_relaciones:    {judicial_rel_copied}')
    print(f'historial:              {historial_copied}')
    print(f'scheduler_config:       {scheduler_copied}')
    print(f'casos:                  {casos_copied}')
    print(f'caso_notas:             {caso_notas_copied}')
    print(f'scraper_log:            {scraper_log_copied}')
    print(f'archivos_objetivo_borrados: {files_deleted}')


if __name__ == '__main__':
    main()
