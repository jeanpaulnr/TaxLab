import argparse
import os
import sqlite3
import sys

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE)

from document_analysis import generate_document_analysis

DB = os.path.join(BASE, 'data', 'sii_normativa.db')


def get_connection():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def main():
    parser = argparse.ArgumentParser(description='Genera document_analysis persistido para uno o varios documentos.')
    parser.add_argument('--doc-id', type=int, help='ID puntual del documento')
    parser.add_argument('--tipo', help='Filtra por tipo documental')
    parser.add_argument('--desde', type=int, help='Anio minimo')
    parser.add_argument('--hasta', type=int, help='Anio maximo')
    parser.add_argument('--limit', type=int, default=20, help='Cantidad maxima de documentos')
    parser.add_argument('--force', action='store_true', help='Regenera aunque ya exista analisis')
    args = parser.parse_args()

    conn = get_connection()
    try:
        if args.doc_id:
            ids = [args.doc_id]
        else:
            where = []
            params = []
            if args.tipo:
                where.append('tipo = ?')
                params.append(args.tipo)
            if args.desde:
                where.append('anio >= ?')
                params.append(args.desde)
            if args.hasta:
                where.append('anio <= ?')
                params.append(args.hasta)
            clause = f"WHERE {' AND '.join(where)}" if where else ''
            rows = conn.execute(
                f"""
                SELECT id
                FROM documentos
                {clause}
                ORDER BY anio DESC, id DESC
                LIMIT ?
                """,
                params + [args.limit],
            ).fetchall()
            ids = [row['id'] for row in rows]
    finally:
        conn.close()

    if not ids:
        print('No se encontraron documentos para analizar.')
        return

    for doc_id in ids:
        analysis = generate_document_analysis(DB, doc_id, force=args.force)
        print(
            f"doc_id={doc_id} status={analysis.get('status')} "
            f"confidence={analysis.get('confidence')} model={analysis.get('model')}"
        )


if __name__ == '__main__':
    main()
