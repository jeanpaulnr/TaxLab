import argparse
import csv
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / 'data' / 'sii_normativa.db'
REPORTS = ROOT / 'reports'
REPORTS.mkdir(exist_ok=True)


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def resolve_pdf_path(stored_path):
    if not stored_path:
        return None
    path = Path(stored_path)
    if path.is_absolute():
        return path
    return ROOT / stored_path.replace('/', os.sep)


def fetch_scalar(conn, sql, params=()):
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else 0


def rows_to_dicts(rows):
    return [dict(r) for r in rows]


def write_csv(path, rows, fieldnames):
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description='Diagnostico de integridad del corpus TaxLab')
    parser.add_argument('--desde', type=int, default=1995)
    parser.add_argument('--hasta', type=int, default=2026)
    parser.add_argument('--sample', type=int, default=10)
    args = parser.parse_args()

    conn = get_conn()
    try:
        params = (args.desde, args.hasta)
        total = fetch_scalar(conn, 'SELECT COUNT(*) FROM documentos WHERE anio BETWEEN ? AND ?', params)
        por_tipo = rows_to_dicts(conn.execute(
            '''SELECT tipo, COUNT(*) AS total
               FROM documentos
               WHERE anio BETWEEN ? AND ?
               GROUP BY tipo
               ORDER BY tipo''', params
        ).fetchall())

        vacios = fetch_scalar(conn, '''
            SELECT COUNT(*) FROM documentos
            WHERE anio BETWEEN ? AND ?
              AND (contenido IS NULL OR trim(contenido)='')
        ''', params)
        chars_cero = fetch_scalar(conn, '''
            SELECT COUNT(*) FROM documentos
            WHERE anio BETWEEN ? AND ?
              AND COALESCE(chars_texto, 0) = 0
        ''', params)
        sin_pdf = fetch_scalar(conn, '''
            SELECT COUNT(*)
            FROM documentos d
            LEFT JOIN judicial_docs jd ON jd.doc_id = d.id
            WHERE d.anio BETWEEN ? AND ?
              AND COALESCE(NULLIF(d.pdf_local, ''), NULLIF(jd.pdf_local, '')) IS NULL
        ''', params)
        paginas_cero = fetch_scalar(conn, '''
            SELECT COUNT(*) FROM documentos
            WHERE anio BETWEEN ? AND ?
              AND COALESCE(paginas, 0) = 0
        ''', params)
        sospechosos_truncados = rows_to_dicts(conn.execute(
            '''SELECT id, tipo, numero, anio, fecha, titulo,
                      length(contenido) AS len_contenido,
                      COALESCE(chars_texto, 0) AS chars_texto,
                      COALESCE(paginas, 0) AS paginas,
                      COALESCE(d.pdf_local, (SELECT jd.pdf_local FROM judicial_docs jd WHERE jd.doc_id=d.id)) AS pdf_local,
                      substr(contenido, 1, 500) AS inicio
               FROM documentos d
               WHERE anio BETWEEN ? AND ?
                 AND length(contenido) BETWEEN 49950 AND 50000
               ORDER BY anio DESC, tipo, numero''', params
        ).fetchall())
        demasiado_cortos = rows_to_dicts(conn.execute(
            '''SELECT id, tipo, numero, anio, fecha, titulo,
                      length(trim(COALESCE(contenido,''))) AS len_contenido,
                      COALESCE(chars_texto, 0) AS chars_texto,
                      COALESCE(paginas, 0) AS paginas,
                      COALESCE(d.pdf_local, (SELECT jd.pdf_local FROM judicial_docs jd WHERE jd.doc_id=d.id)) AS pdf_local,
                      substr(contenido, 1, 500) AS inicio
               FROM documentos d
               WHERE anio BETWEEN ? AND ?
                 AND length(trim(COALESCE(contenido,''))) < 1500
               ORDER BY anio DESC, tipo, numero
               LIMIT 500''', params
        ).fetchall())
        top_largos = rows_to_dicts(conn.execute(
            '''SELECT id, tipo, numero, anio, fecha, titulo,
                      length(COALESCE(contenido,'')) AS len_contenido,
                      COALESCE(chars_texto, 0) AS chars_texto,
                      COALESCE(paginas, 0) AS paginas,
                      COALESCE(d.pdf_local, (SELECT jd.pdf_local FROM judicial_docs jd WHERE jd.doc_id=d.id)) AS pdf_local,
                      substr(contenido, 1, 300) AS inicio
               FROM documentos d
               WHERE anio BETWEEN ? AND ?
               ORDER BY len_contenido DESC, chars_texto DESC
               LIMIT 50''', params
        ).fetchall())
        muestra = rows_to_dicts(conn.execute(
            '''SELECT id, tipo, numero, anio, fecha, titulo,
                      COALESCE(paginas, 0) AS paginas,
                      COALESCE(chars_texto, 0) AS chars_texto,
                      COALESCE(d.pdf_local, (SELECT jd.pdf_local FROM judicial_docs jd WHERE jd.doc_id=d.id)) AS pdf_local,
                      substr(COALESCE(contenido,''), 1, 300) AS inicio
               FROM documentos d
               WHERE anio BETWEEN ? AND ?
               ORDER BY RANDOM()
               LIMIT ?''', (args.desde, args.hasta, args.sample)
        ).fetchall())
        per_year = rows_to_dicts(conn.execute(
            '''SELECT tipo, anio,
                      COUNT(*) AS docs,
                      SUM(CASE WHEN length(COALESCE(contenido,'')) BETWEEN 49950 AND 50000 THEN 1 ELSE 0 END) AS sospechosos_truncados,
                      SUM(CASE WHEN COALESCE(chars_texto,0)=0 THEN 1 ELSE 0 END) AS sin_chars,
                      SUM(CASE WHEN COALESCE(paginas,0)=0 THEN 1 ELSE 0 END) AS sin_paginas,
                      MIN(length(COALESCE(contenido,''))) AS min_len,
                      ROUND(AVG(length(COALESCE(contenido,''))), 1) AS avg_len,
                      MAX(length(COALESCE(contenido,''))) AS max_len
               FROM documentos
               WHERE anio BETWEEN ? AND ?
               GROUP BY tipo, anio
               ORDER BY anio DESC, tipo''', params
        ).fetchall())

        pdf_faltantes_detalle = []
        for row in conn.execute(
            '''SELECT d.id, d.tipo, d.numero, d.anio, d.fecha, d.titulo,
                      COALESCE(d.pdf_local, (SELECT jd.pdf_local FROM judicial_docs jd WHERE jd.doc_id=d.id)) AS pdf_local,
                      substr(COALESCE(d.contenido,''), 1, 300) AS inicio
               FROM documentos d
               WHERE d.anio BETWEEN ? AND ?''', params
        ):
            rec = dict(row)
            pdf_path = resolve_pdf_path(rec['pdf_local']) if rec['pdf_local'] else None
            rec['pdf_exists'] = bool(pdf_path and pdf_path.exists())
            if not rec['pdf_exists']:
                pdf_faltantes_detalle.append(rec)

        resumen = {
            'generated_at': datetime.now().isoformat(),
            'db_path': str(DB_PATH),
            'desde': args.desde,
            'hasta': args.hasta,
            'total_documentos': total,
            'por_tipo': por_tipo,
            'documentos_contenido_vacio': vacios,
            'documentos_chars_cero': chars_cero,
            'documentos_sin_pdf': len(pdf_faltantes_detalle),
            'documentos_paginas_cero': paginas_cero,
            'sospechosos_truncacion': len(sospechosos_truncados),
            'demasiado_cortos': len(demasiado_cortos),
        }

        stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        resumen_path = REPORTS / f'diagnostico_integridad_resumen_{stamp}.json'
        sospechosos_path = REPORTS / f'diagnostico_integridad_sospechosos_{stamp}.csv'
        muestra_path = REPORTS / f'diagnostico_integridad_muestra_{stamp}.csv'
        top_path = REPORTS / f'diagnostico_integridad_top_largos_{stamp}.csv'
        per_year_path = REPORTS / f'diagnostico_integridad_por_anio_{stamp}.csv'
        faltantes_pdf_path = REPORTS / f'diagnostico_integridad_sin_pdf_{stamp}.csv'

        resumen_path.write_text(json.dumps({
            'resumen': resumen,
            'por_anio': per_year,
        }, ensure_ascii=False, indent=2), encoding='utf-8')

        if sospechosos_truncados:
            write_csv(sospechosos_path, sospechosos_truncados, sospechosos_truncados[0].keys())
        else:
            write_csv(sospechosos_path, [], ['id','tipo','numero','anio','fecha','titulo','len_contenido','chars_texto','paginas','pdf_local','inicio'])
        if muestra:
            write_csv(muestra_path, muestra, muestra[0].keys())
        else:
            write_csv(muestra_path, [], ['id','tipo','numero','anio','fecha','titulo','paginas','chars_texto','pdf_local','inicio'])
        if top_largos:
            write_csv(top_path, top_largos, top_largos[0].keys())
        else:
            write_csv(top_path, [], ['id','tipo','numero','anio','fecha','titulo','len_contenido','chars_texto','paginas','pdf_local','inicio'])
        if per_year:
            write_csv(per_year_path, per_year, per_year[0].keys())
        else:
            write_csv(per_year_path, [], ['tipo','anio','docs','sospechosos_truncados','sin_chars','sin_paginas','min_len','avg_len','max_len'])
        if pdf_faltantes_detalle:
            write_csv(faltantes_pdf_path, pdf_faltantes_detalle, pdf_faltantes_detalle[0].keys())
        else:
            write_csv(faltantes_pdf_path, [], ['id','tipo','numero','anio','fecha','titulo','pdf_local','inicio','pdf_exists'])

        print('=' * 72)
        print('  DIAGNOSTICO DE INTEGRIDAD DEL CORPUS')
        print('=' * 72)
        print(f'Base SQLite:              {DB_PATH}')
        print(f'Rango auditado:           {args.desde}-{args.hasta}')
        print(f'Total documentos:         {total}')
        print(f'Contenido vacio:          {vacios}')
        print(f'chars_texto = 0/NULL:     {chars_cero}')
        print(f'paginas = 0/NULL:         {paginas_cero}')
        print(f'Sin PDF asociado:         {len(pdf_faltantes_detalle)}')
        print(f'Sospechosos truncacion:   {len(sospechosos_truncados)}')
        print(f'Demasiado cortos (<1500): {len(demasiado_cortos)}')
        print('-' * 72)
        print('Por tipo:')
        for row in por_tipo:
            print(f"  - {row['tipo']:<12} {row['total']:>6}")
        print('-' * 72)
        print(f'Resumen JSON:             {resumen_path}')
        print(f'Sospechosos CSV:          {sospechosos_path}')
        print(f'Muestra CSV:              {muestra_path}')
        print(f'Top largos CSV:           {top_path}')
        print(f'Por anio CSV:             {per_year_path}')
        print(f'Sin PDF CSV:              {faltantes_pdf_path}')
    finally:
        conn.close()


if __name__ == '__main__':
    main()

