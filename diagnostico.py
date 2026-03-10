"""
SII Normativa — Diagnóstico completo
Corre desde la raíz del proyecto: python diagnostico.py
"""
import sqlite3, os, requests

BASE = os.path.dirname(os.path.abspath(__file__))
DB   = os.path.join(BASE, 'data', 'sii_normativa.db')

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
c = conn.cursor()

print("=" * 70)
print("  DIAGNÓSTICO SII NORMATIVA")
print("=" * 70)

# 1. Total
c.execute("SELECT COUNT(*) FROM documentos")
print(f"\n📊 TOTAL DOCUMENTOS: {c.fetchone()[0]}")

# 2. Por tipo
print("\n📋 POR TIPO:")
c.execute("SELECT tipo, COUNT(*) cnt FROM documentos GROUP BY tipo ORDER BY cnt DESC")
for r in c.fetchall():
    print(f"   {r['tipo']:20s} → {r['cnt']:,}")

# 3. Por tipo y año
print("\n📅 POR TIPO Y AÑO:")
c.execute("""
    SELECT tipo, anio, COUNT(*) cnt 
    FROM documentos 
    GROUP BY tipo, anio 
    ORDER BY tipo, anio DESC
""")
for r in c.fetchall():
    print(f"   {r['tipo']:20s} {r['anio']}  → {r['cnt']:>5,}")

# 4. Oficios OTRAS
print("\n🔍 OFICIOS 'OTRAS NORMAS':")
c.execute("SELECT COUNT(*) FROM documentos WHERE subtema LIKE '%OTRAS%'")
print(f"   Con subtema OTRAS: {c.fetchone()[0]}")
c.execute("SELECT DISTINCT substr(subtema,1,30) as sub FROM documentos WHERE tipo='oficio' LIMIT 10")
print("   Subtemas encontrados en oficios:")
for r in c.fetchall():
    print(f"     - {r['sub']}")

# 5. Ejemplos de documentos
print("\n📄 EJEMPLO CIRCULAR:")
c.execute("""SELECT id,tipo,numero,anio,fecha,substr(titulo,1,80) titulo,
             substr(resumen,1,120) resumen, leyes_citadas, articulos_clave
             FROM documentos WHERE tipo='circular' ORDER BY anio DESC LIMIT 1""")
r = c.fetchone()
if r:
    for k in r.keys(): print(f"   {k}: {r[k]}")

print("\n📄 EJEMPLO OFICIO:")
c.execute("""SELECT id,tipo,numero,anio,fecha,substr(titulo,1,80) titulo,
             substr(resumen,1,120) resumen, leyes_citadas, articulos_clave
             FROM documentos WHERE tipo='oficio' ORDER BY anio DESC LIMIT 1""")
r = c.fetchone()
if r:
    for k in r.keys(): print(f"   {k}: {r[k]}")

print("\n📄 EJEMPLO RESOLUCIÓN:")
c.execute("""SELECT id,tipo,numero,anio,fecha,substr(titulo,1,80) titulo
             FROM documentos WHERE tipo='resolucion' LIMIT 1""")
r = c.fetchone()
if r:
    for k in r.keys(): print(f"   {k}: {r[k]}")
else:
    print("   ⚠ NO HAY RESOLUCIONES EN LA BASE")

# 6. Oficios LIR 2025 - errores
print("\n🔍 SCRAPER LOG — OFICIOS LIR 2025:")
try:
    c.execute("""SELECT estado, COUNT(*) cnt FROM scraper_log 
                 WHERE tipo='oficio_lir' AND anio=2025 GROUP BY estado""")
    for r in c.fetchall():
        print(f"   {r['estado']:20s} → {r['cnt']}")
except:
    print("   (tabla scraper_log no disponible)")

# 7. Probar URLs de resoluciones
print("\n🌐 PROBANDO URLs DE RESOLUCIONES:")
urls_test = [
    ("indres2025.htm (estándar)", "https://www.sii.cl/normativa_legislacion/resoluciones/2025/indres2025.htm"),
    ("indres2024.htm", "https://www.sii.cl/normativa_legislacion/resoluciones/2024/indres2024.htm"),
    ("indres2023.htm", "https://www.sii.cl/normativa_legislacion/resoluciones/2023/indres2023.htm"),
    ("indreso2025.htm (variante)", "https://www.sii.cl/normativa_legislacion/resoluciones/2025/indreso2025.htm"),
    ("sin subcarpeta año", "https://www.sii.cl/normativa_legislacion/resoluciones/indres2025.htm"),
    ("página principal resol", "https://www.sii.cl/normativa_legislacion/resoluciones.htm"),
    ("jurisprudencia admin", "https://www.sii.cl/normativa_legislacion/jurisprudencia_administrativa/"),
    ("índice jadm", "https://www.sii.cl/normativa_legislacion/jurisprudencia_administrativa/indice_jadm.htm"),
]

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

for nombre, url in urls_test:
    try:
        r = requests.get(url, headers=headers, timeout=10, allow_redirects=True)
        status = r.status_code
        size = len(r.content)
        has_pdf = 'reso' in r.text.lower()[:5000] if status == 200 else False
        print(f"   {status}  {size:>8,} bytes  pdf_links={'SÍ' if has_pdf else 'NO':3s}  {nombre}")
        if status == 200 and size > 500:
            # Guardar para inspección
            fname = nombre.replace(" ", "_").replace("/","_").replace("(","").replace(")","")[:30] + ".html"
            with open(os.path.join(BASE, 'logs', fname), 'w', encoding='utf-8') as f:
                f.write(r.text[:10000])
    except Exception as e:
        print(f"   ERR  {nombre}: {e}")

# 8. Schema actual
print("\n🗄️ SCHEMA ACTUAL:")
c.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='documentos'")
r = c.fetchone()
if r: print(f"   {r[0][:300]}...")

conn.close()
print("\n" + "=" * 70)
print("  FIN DIAGNÓSTICO")
print("=" * 70)
