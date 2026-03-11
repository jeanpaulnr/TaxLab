import os
import sqlite3

BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, 'data', 'sii_normativa.db')

DOCUMENTO_COLUMNS = {
    'organo_emisor': 'TEXT',
    'criterio_principal': 'TEXT',
    'sentido_criterio': 'TEXT',
    'tema_central': 'TEXT',
    'documento_relacionado': 'TEXT',
}

CASOS_SQL = """
CREATE TABLE IF NOT EXISTS casos (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre             TEXT NOT NULL,
    rut_cliente        TEXT,
    descripcion        TEXT,
    estado             TEXT DEFAULT 'activo',
    fecha_creacion     TEXT DEFAULT (datetime('now')),
    fecha_modificacion TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS caso_notas (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    caso_id   INTEGER REFERENCES casos(id) ON DELETE CASCADE,
    contenido TEXT,
    tipo      TEXT DEFAULT 'nota',
    doc_id    INTEGER REFERENCES documentos(id),
    fecha     TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_casos_estado ON casos(estado);
CREATE INDEX IF NOT EXISTS idx_caso_notas_caso ON caso_notas(caso_id);
CREATE INDEX IF NOT EXISTS idx_caso_notas_doc ON caso_notas(doc_id);
"""


def get_connection(db_path=DB):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn, table_name):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def column_exists(conn, table_name, column_name):
    rows = conn.execute(f'PRAGMA table_info({table_name})').fetchall()
    return any(row['name'] == column_name for row in rows)


def run_migrations(db_path=DB):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = get_connection(db_path)
    applied = []
    try:
        conn.executescript(CASOS_SQL)
        applied.append('tablas_casos')

        if table_exists(conn, 'documentos'):
            for column_name, column_type in DOCUMENTO_COLUMNS.items():
                if column_exists(conn, 'documentos', column_name):
                    continue
                conn.execute(f'ALTER TABLE documentos ADD COLUMN {column_name} {column_type}')
                applied.append(f'documentos.{column_name}')

        conn.commit()
        return applied
    finally:
        conn.close()


if __name__ == '__main__':
    cambios = run_migrations()
    print('Migraciones aplicadas:')
    for cambio in cambios:
        print(f' - {cambio}')
