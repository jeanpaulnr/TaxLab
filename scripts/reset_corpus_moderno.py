import argparse
import shutil
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / 'data' / 'sii_normativa.db'
PDF_ROOT = ROOT / 'pdfs'
HTML_ROOT = ROOT / 'html_historico'
IMG_ROOT = ROOT / 'img_historico'
OCR_ROOT = ROOT / 'ocr_historico'
DOC_ROOT = ROOT / 'doc_historico'

LOG_TYPES = ('circular', 'resolucion', 'oficio_iva', 'oficio_lir', 'oficio_otras', 'circular_historica')
TARGET_TYPES = ('circular', 'oficio', 'resolucion')
BATCH_SIZE = 200


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def count_targets(conn, desde, hasta):
    return conn.execute(
        '''SELECT tipo, COUNT(*) AS total
           FROM documentos
           WHERE anio BETWEEN ? AND ?
             AND tipo IN ('circular','oficio','resolucion')
           GROUP BY tipo
           ORDER BY tipo''',
        (desde, hasta),
    ).fetchall()


def delete_year_dir(base: Path, year: int) -> int:
    path = base / str(year)
    if not path.exists():
        return 0
    count = sum(1 for p in path.rglob('*') if p.is_file())
    shutil.rmtree(path)
    return count


def batched(seq, size):
    for idx in range(0, len(seq), size):
        yield seq[idx:idx + size]


def reset_modern_nonjudicial(desde: int, hasta: int) -> dict:
    conn = get_conn()
    docs_deleted = 0
    logs_deleted = 0
    try:
        ids = [
            row['id'] for row in conn.execute(
                '''SELECT id FROM documentos
                   WHERE anio BETWEEN ? AND ?
                     AND tipo IN ('circular','oficio','resolucion')
                   ORDER BY id''',
                (desde, hasta),
            ).fetchall()
        ]

        for batch in batched(ids, BATCH_SIZE):
            placeholders = ','.join('?' for _ in batch)
            conn.execute('BEGIN')
            conn.execute(
                f'''DELETE FROM articulos_idx
                    WHERE doc_id IN ({placeholders})''',
                batch,
            )
            docs_deleted += conn.execute(
                f'''DELETE FROM documentos
                    WHERE id IN ({placeholders})''',
                batch,
            ).rowcount
            conn.commit()

        conn.execute('BEGIN')
        logs_deleted = conn.execute(
            f'''DELETE FROM scraper_log
                WHERE anio BETWEEN ? AND ?
                  AND tipo IN ({','.join('?' for _ in LOG_TYPES)})''',
            (desde, hasta, *LOG_TYPES),
        ).rowcount
        try:
            conn.execute("INSERT INTO docs_fts(docs_fts) VALUES('rebuild')")
        except Exception:
            pass
        conn.commit()
    finally:
        conn.close()

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

    return {
        'docs_deleted': docs_deleted,
        'logs_deleted': logs_deleted,
        'files_deleted': files_deleted,
    }


def reset_full() -> dict:
    deleted = {'db_removed': False, 'files_deleted': 0}
    for suffix in ('', '-wal', '-shm'):
        path = Path(str(DB_PATH) + suffix)
        if path.exists():
            path.unlink()
            if suffix == '':
                deleted['db_removed'] = True

    for base in (PDF_ROOT, HTML_ROOT, IMG_ROOT, OCR_ROOT, DOC_ROOT):
        if base.exists():
            deleted['files_deleted'] += sum(1 for p in base.rglob('*') if p.is_file())
            shutil.rmtree(base)
    return deleted


def main():
    parser = argparse.ArgumentParser(description='Reset controlado del corpus moderno')
    parser.add_argument('--scope', choices=['modern-nonjudicial', 'full'], default='modern-nonjudicial')
    parser.add_argument('--desde', type=int, default=1995)
    parser.add_argument('--hasta', type=int, default=2026)
    parser.add_argument('--yes', action='store_true', help='Ejecutar de verdad')
    args = parser.parse_args()

    print('=' * 72)
    print('  RESET CONTROLADO DEL CORPUS')
    print('=' * 72)
    print(f'Scope: {args.scope}')
    if args.scope == 'modern-nonjudicial':
        conn = get_conn()
        try:
            rows = count_targets(conn, args.desde, args.hasta)
        finally:
            conn.close()
        total = sum(row['total'] for row in rows)
        print(f'Rango: {args.desde}-{args.hasta}')
        print(f'Documentos objetivo: {total}')
        for row in rows:
            print(f"  - {row['tipo']}: {row['total']}")
        print('Se eliminaran tambien PDFs circular/oficio/resolucion y auxiliares historicos 1995-2012.')
    else:
        print(f'Base objetivo: {DB_PATH}')
        print(f'Carpetas objetivo: {PDF_ROOT}, {HTML_ROOT}, {IMG_ROOT}, {OCR_ROOT}, {DOC_ROOT}')

    if not args.yes:
        print('-' * 72)
        print('Dry run. Agrega --yes para ejecutar.')
        return

    if args.scope == 'modern-nonjudicial':
        resultado = reset_modern_nonjudicial(args.desde, args.hasta)
    else:
        resultado = reset_full()

    print('-' * 72)
    for key, value in resultado.items():
        print(f'{key}: {value}')


if __name__ == '__main__':
    main()
