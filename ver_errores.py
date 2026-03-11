import sqlite3
conn = sqlite3.connect('data/sii_normativa.db')
rows = conn.execute("""
    SELECT anio, numero, estado, url 
    FROM scraper_log 
    WHERE tipo='resolucion' AND estado != 'ok'
    ORDER BY anio DESC
""").fetchall()
print(f"\n  {len(rows)} resoluciones con error:\n")
for r in rows:
    print(f"  Reso N°{r[1]}/{r[0]} — {r[2]}")
    print(f"    URL: {r[3]}")
conn.close()