# -*- coding: utf-8 -*-
"""
SII Normativa - Diagnostico completo
Corre desde la raiz del proyecto: python diagnostico.py
"""

import os
import sqlite3
import requests

BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, "data", "sii_normativa.db")
LOGS = os.path.join(BASE, "logs")
os.makedirs(LOGS, exist_ok=True)


OFICIO_CASE_SQL = """
CASE
    WHEN tipo = 'oficio' AND UPPER(COALESCE(subtema, '')) LIKE 'RENTA%' THEN 'oficio / LIR'
    WHEN tipo = 'oficio' AND UPPER(COALESCE(subtema, '')) LIKE 'IVA%' THEN 'oficio / IVA'
    WHEN tipo = 'oficio' AND (
        UPPER(COALESCE(subtema, '')) LIKE 'OTRAS%' OR
        UPPER(COALESCE(subtema, '')) LIKE 'OTROS%'
    ) THEN 'oficio / OTRAS NORMAS'
    WHEN tipo = 'oficio' THEN 'oficio / SIN SUBCLASIFICAR'
    ELSE tipo
END
"""


def print_header(title):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_kv_row(label, value):
    print(f"   {label:28s} -> {value}")


conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
c = conn.cursor()

print("=" * 70)
print("  DIAGNOSTICO SII NORMATIVA")
print("=" * 70)

# 1. Total
c.execute("SELECT COUNT(*) AS total FROM documentos")
print(f"\nTOTAL DOCUMENTOS: {c.fetchone()['total']:,}")

# 2. Por tipo principal
print("\nPOR TIPO:")
c.execute(
    """
    SELECT tipo, COUNT(*) AS cnt
    FROM documentos
    GROUP BY tipo
    ORDER BY cnt DESC, tipo
    """
)
for r in c.fetchall():
    print_kv_row(r["tipo"], f"{r['cnt']:,}")

# 3. Por subclasificacion (oficios abiertos en ramas)
print("\nPOR SUBCLASIFICACION:")
c.execute(
    f"""
    SELECT {OFICIO_CASE_SQL} AS tipo_detalle, COUNT(*) AS cnt
    FROM documentos
    GROUP BY tipo_detalle
    ORDER BY
        CASE
            WHEN tipo_detalle = 'circular' THEN 1
            WHEN tipo_detalle = 'resolucion' THEN 2
            WHEN tipo_detalle = 'judicial' THEN 3
            WHEN tipo_detalle = 'oficio / LIR' THEN 4
            WHEN tipo_detalle = 'oficio / IVA' THEN 5
            WHEN tipo_detalle = 'oficio / OTRAS NORMAS' THEN 6
            WHEN tipo_detalle = 'oficio / SIN SUBCLASIFICAR' THEN 7
            ELSE 99
        END,
        tipo_detalle
    """
)
for r in c.fetchall():
    print_kv_row(r["tipo_detalle"], f"{r['cnt']:,}")

# 4. Por subclasificacion y anio
print("\nPOR SUBCLASIFICACION Y ANIO:")
c.execute(
    f"""
    SELECT {OFICIO_CASE_SQL} AS tipo_detalle, anio, COUNT(*) AS cnt
    FROM documentos
    GROUP BY tipo_detalle, anio
    ORDER BY
        CASE
            WHEN tipo_detalle = 'circular' THEN 1
            WHEN tipo_detalle = 'resolucion' THEN 2
            WHEN tipo_detalle = 'judicial' THEN 3
            WHEN tipo_detalle = 'oficio / LIR' THEN 4
            WHEN tipo_detalle = 'oficio / IVA' THEN 5
            WHEN tipo_detalle = 'oficio / OTRAS NORMAS' THEN 6
            WHEN tipo_detalle = 'oficio / SIN SUBCLASIFICAR' THEN 7
            ELSE 99
        END,
        anio DESC
    """
)
for r in c.fetchall():
    anio = r["anio"] if r["anio"] is not None else "NULL"
    print(f"   {r['tipo_detalle']:28s} {anio} -> {r['cnt']:>5,}")

# 5. Desglose especifico de oficios
print("\nOFICIOS POR RAMA:")
c.execute(
    """
    SELECT
        CASE
            WHEN UPPER(COALESCE(subtema, '')) LIKE 'RENTA%' THEN 'LIR'
            WHEN UPPER(COALESCE(subtema, '')) LIKE 'IVA%' THEN 'IVA'
            WHEN UPPER(COALESCE(subtema, '')) LIKE 'OTRAS%' OR UPPER(COALESCE(subtema, '')) LIKE 'OTROS%' THEN 'OTRAS NORMAS'
            ELSE 'SIN SUBCLASIFICAR'
        END AS rama,
        COUNT(*) AS cnt
    FROM documentos
    WHERE tipo = 'oficio'
    GROUP BY rama
    ORDER BY
        CASE rama
            WHEN 'LIR' THEN 1
            WHEN 'IVA' THEN 2
            WHEN 'OTRAS NORMAS' THEN 3
            ELSE 99
        END
    """
)
for r in c.fetchall():
    print_kv_row(r["rama"], f"{r['cnt']:,}")

print("\nOFICIOS POR RAMA Y ANIO:")
c.execute(
    """
    SELECT
        CASE
            WHEN UPPER(COALESCE(subtema, '')) LIKE 'RENTA%' THEN 'LIR'
            WHEN UPPER(COALESCE(subtema, '')) LIKE 'IVA%' THEN 'IVA'
            WHEN UPPER(COALESCE(subtema, '')) LIKE 'OTRAS%' OR UPPER(COALESCE(subtema, '')) LIKE 'OTROS%' THEN 'OTRAS NORMAS'
            ELSE 'SIN SUBCLASIFICAR'
        END AS rama,
        anio,
        COUNT(*) AS cnt
    FROM documentos
    WHERE tipo = 'oficio'
    GROUP BY rama, anio
    ORDER BY
        CASE rama
            WHEN 'LIR' THEN 1
            WHEN 'IVA' THEN 2
            WHEN 'OTRAS NORMAS' THEN 3
            ELSE 99
        END,
        anio DESC
    """
)
for r in c.fetchall():
    anio = r["anio"] if r["anio"] is not None else "NULL"
    print(f"   {'oficio / ' + r['rama']:28s} {anio} -> {r['cnt']:>5,}")

# 6. Ejemplos de documentos
print("\nEJEMPLO CIRCULAR:")
c.execute(
    """
    SELECT id, tipo, numero, anio, fecha, substr(titulo,1,80) AS titulo,
           substr(resumen,1,120) AS resumen, leyes_citadas, articulos_clave
    FROM documentos
    WHERE tipo='circular'
    ORDER BY anio DESC, CAST(numero AS INTEGER) DESC
    LIMIT 1
    """
)
r = c.fetchone()
if r:
    for k in r.keys():
        print(f"   {k}: {r[k]}")

for rama, patron in (
    ("OFICIO LIR", "RENTA%"),
    ("OFICIO IVA", "IVA%"),
    ("OFICIO OTRAS NORMAS", "OTRAS%"),
):
    print(f"\nEJEMPLO {rama}:")
    c.execute(
        """
        SELECT id, tipo, numero, anio, fecha, substr(titulo,1,80) AS titulo,
               substr(resumen,1,120) AS resumen, subtema, leyes_citadas, articulos_clave
        FROM documentos
        WHERE tipo='oficio' AND UPPER(COALESCE(subtema,'')) LIKE ?
        ORDER BY anio DESC, CAST(numero AS INTEGER) DESC
        LIMIT 1
        """,
        (patron,),
    )
    r = c.fetchone()
    if r:
        for k in r.keys():
            print(f"   {k}: {r[k]}")
    else:
        print("   SIN REGISTROS")

print("\nEJEMPLO RESOLUCION:")
c.execute(
    """
    SELECT id, tipo, numero, anio, fecha, substr(titulo,1,80) AS titulo
    FROM documentos
    WHERE tipo='resolucion'
    ORDER BY anio DESC, CAST(numero AS INTEGER) DESC
    LIMIT 1
    """
)
r = c.fetchone()
if r:
    for k in r.keys():
        print(f"   {k}: {r[k]}")
else:
    print("   NO HAY RESOLUCIONES EN LA BASE")

print("\nEJEMPLO JUDICIAL:")
c.execute(
    """
    SELECT id, tipo, numero, anio, fecha, substr(titulo,1,80) AS titulo,
           substr(resumen,1,120) AS resumen
    FROM documentos
    WHERE tipo='judicial'
    ORDER BY anio DESC, id DESC
    LIMIT 1
    """
)
r = c.fetchone()
if r:
    for k in r.keys():
        print(f"   {k}: {r[k]}")
else:
    print("   NO HAY JURISPRUDENCIA JUDICIAL EN LA BASE")

# 7. Scraper log por rama de oficios
print("\nSCRAPER LOG - OFICIOS 2025:")
try:
    for tipo_log in ("oficio_lir", "oficio_iva", "oficio_otras"):
        print(f"   {tipo_log}:")
        c.execute(
            """
            SELECT estado, COUNT(*) AS cnt
            FROM scraper_log
            WHERE tipo=? AND anio=2025
            GROUP BY estado
            ORDER BY cnt DESC, estado
            """,
            (tipo_log,),
        )
        rows = c.fetchall()
        if not rows:
            print("     (sin eventos)")
        for r in rows:
            print(f"     {r['estado']:20s} -> {r['cnt']}")
except Exception:
    print("   (tabla scraper_log no disponible)")

# 8. Probar URLs de resoluciones
print("\nPROBANDO URLs DE RESOLUCIONES:")
urls_test = [
    ("indres2025.htm (estandar)", "https://www.sii.cl/normativa_legislacion/resoluciones/2025/indres2025.htm"),
    ("indres2024.htm", "https://www.sii.cl/normativa_legislacion/resoluciones/2024/indres2024.htm"),
    ("indres2023.htm", "https://www.sii.cl/normativa_legislacion/resoluciones/2023/indres2023.htm"),
    ("indreso2025.htm (variante)", "https://www.sii.cl/normativa_legislacion/resoluciones/2025/indreso2025.htm"),
    ("sin subcarpeta anio", "https://www.sii.cl/normativa_legislacion/resoluciones/indres2025.htm"),
    ("pagina principal resol", "https://www.sii.cl/normativa_legislacion/resoluciones.htm"),
    ("jurisprudencia admin", "https://www.sii.cl/normativa_legislacion/jurisprudencia_administrativa/"),
    ("indice jadm", "https://www.sii.cl/normativa_legislacion/jurisprudencia_administrativa/indice_jadm.htm"),
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

for nombre, url in urls_test:
    try:
        r = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
        status = r.status_code
        size = len(r.content)
        has_pdf = "reso" in r.text.lower()[:5000] if status == 200 else False
        print(f"   {status}  {size:>8,} bytes  pdf_links={'SI' if has_pdf else 'NO':3s}  {nombre}")
        if status == 200 and size > 500:
            fname = (
                nombre.replace(" ", "_")
                .replace("/", "_")
                .replace("(", "")
                .replace(")", "")[:30]
                + ".html"
            )
            with open(os.path.join(LOGS, fname), "w", encoding="utf-8") as f:
                f.write(r.text[:10000])
    except Exception as e:
        print(f"   ERR  {nombre}: {e}")

# 9. Schema actual
print("\nSCHEMA ACTUAL:")
c.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='documentos'")
r = c.fetchone()
if r:
    print(f"   {r[0][:300]}...")

conn.close()
print("\n" + "=" * 70)
print("  FIN DIAGNOSTICO")
print("=" * 70)
