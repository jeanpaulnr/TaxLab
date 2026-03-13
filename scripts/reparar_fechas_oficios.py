import re
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "sii_normativa.db"


def extraer_fecha_oficio(texto: str, numero: str | None = None) -> str | None:
    encabezado = (texto or "")[:3000]
    patrones = []
    if numero:
        nro = re.escape(str(numero).strip())
        patrones.extend(
            [
                rf"ORD\.\s*N[°ºo]?\s*{nro}\s*,?\s*DE\s*(\d{{1,2}}[./-]\d{{1,2}}[./-]\d{{4}})",
                rf"OFICIO\s+ORDINARIO\s*N[°ºo]?\s*{nro}\s*,?\s*DE\s*(\d{{1,2}}[./-]\d{{1,2}}[./-]\d{{4}})",
                rf"N[°ºo]?\s*{nro}\s*,?\s*DE\s*(\d{{1,2}}[./-]\d{{1,2}}[./-]\d{{4}})",
            ]
        )
    patrones.append(r"ORD\.\s*N[°ºo]?\s*\d+\s*,?\s*DE\s*(\d{1,2}[./-]\d{1,2}[./-]\d{4})")

    for patron in patrones:
        m = re.search(patron, encabezado, re.IGNORECASE)
        if not m:
            continue
        fecha_bruta = m.group(1).replace("/", ".").replace("-", ".")
        m_fecha = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", fecha_bruta)
        if m_fecha:
            d, mo, y = m_fecha.groups()
            return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"
    return None


def corregir():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute(
        """
        SELECT id, numero, anio, fecha, referencia, contenido
        FROM documentos
        WHERE tipo='oficio'
          AND fecha IS NOT NULL
          AND (
              substr(fecha, 1, 4) > printf('%04d', anio)
              OR substr(fecha, 1, 4) > '2026'
          )
        ORDER BY anio DESC, fecha DESC, id DESC
        """
    ).fetchall()

    actualizados = 0
    for row in rows:
        fecha_nueva = extraer_fecha_oficio(row["contenido"] or "", row["numero"])
        if not fecha_nueva:
            continue
        if fecha_nueva == row["fecha"]:
            continue
        referencia = f'Oficio Ordinario N°{row["numero"]}, de {fecha_nueva}'
        cur.execute(
            """
            UPDATE documentos
            SET fecha = ?, referencia = ?
            WHERE id = ?
            """,
            (fecha_nueva, referencia, row["id"]),
        )
        print(f"[ok] id={row['id']} oficio {row['numero']}/{row['anio']} {row['fecha']} -> {fecha_nueva}")
        actualizados += 1

    conn.commit()
    conn.close()
    print(f"actualizados={actualizados}")


if __name__ == "__main__":
    corregir()
