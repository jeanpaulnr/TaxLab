import json
import os
import sqlite3

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(BASE, 'data', 'sii_normativa.db')

import sys
sys.path.insert(0, BASE)
sys.path.insert(0, os.path.join(BASE, 'scraper'))

from engine import _indexar_articulos_conservador, detectar_articulos, detectar_leyes  # noqa: E402


def get_connection():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def main():
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, contenido
            FROM documentos
            WHERE contenido IS NOT NULL AND trim(contenido) <> ''
            ORDER BY id
            """
        ).fetchall()

        total = len(rows)
        for idx, row in enumerate(rows, start=1):
            texto = row['contenido'] or ''
            leyes = detectar_leyes(texto)
            articulos = detectar_articulos(texto)[:20]
            conn.execute(
                """
                UPDATE documentos
                SET leyes_citadas = ?, articulos_clave = ?
                WHERE id = ?
                """,
                (
                    json.dumps(leyes, ensure_ascii=False),
                    json.dumps(articulos, ensure_ascii=False),
                    row['id'],
                ),
            )
            _indexar_articulos_conservador(conn, row['id'], leyes, articulos)
            if idx % 250 == 0:
                conn.commit()
                print(f'{idx}/{total} documentos reindexados...')

        conn.commit()
        print(f'Reindexacion completa: {total} documentos.')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
