# -*- coding: utf-8 -*-
r"""
Diagnóstico de paridad de oficios SII vs base local.

Mide dos cosas:
1. Paridad por rama SII (RENTA / IVA / OTROS) contra la subclasificación local.
2. Paridad por idBlob único del SII, que es la mejor medida de documentos realmente faltantes.

Uso:
  python .\scripts\diagnostico_paridad_oficios.py
  python .\scripts\diagnostico_paridad_oficios.py --desde 2019 --hasta 2026 --detallar
"""

import argparse
import csv
import json
import os
import re
import sqlite3
from datetime import datetime

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "sii_normativa.db")
REPORTS = os.path.join(ROOT, "reports")
os.makedirs(REPORTS, exist_ok=True)

SII_API = "https://www3.sii.cl"
MAP = {
    "RENTA": "RENTA",
    "IVA": "IVA",
    "OTROS": "OTRAS",
}


def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn, name):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def extraer_blob(url_sii):
    if not url_sii:
        return None
    m = re.search(r"id=([^&]+)", url_sii)
    return m.group(1) if m else None


def fetch_api_items(session, anio, key):
    response = session.post(
        f"{SII_API}/getPublicacionesCTByMateria",
        data=json.dumps({"key": key, "year": str(anio)}),
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if response.status_code != 200:
        return []
    return response.json()


def main():
    parser = argparse.ArgumentParser(description="Diagnóstico de paridad de oficios")
    parser.add_argument("--desde", type=int, default=2019)
    parser.add_argument("--hasta", type=int, default=2026)
    parser.add_argument("--detallar", action="store_true", help="Mostrar faltantes por idBlob")
    args = parser.parse_args()

    conn = get_db()
    session = requests.Session()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    detalle_path = os.path.join(REPORTS, f"paridad_oficios_detalle_{stamp}.csv")
    resumen_path = os.path.join(REPORTS, f"paridad_oficios_resumen_{stamp}.json")

    rows_csv = []
    resumen_json = {"desde": args.desde, "hasta": args.hasta, "years": []}

    print("=" * 72)
    print("PARIDAD OFICIOS SII vs BASE")
    print("=" * 72)

    for anio in range(args.hasta, args.desde - 1, -1):
        print(f"\nAÑO {anio}")
        year_summary = {
            "anio": anio,
            "categorias": {},
            "api_unique_blobs": 0,
            "db_unique_blobs": 0,
            "missing_unique_blobs": 0,
            "missing_unique_items": [],
        }

        api_counts_total = 0
        db_counts_total = 0
        api_blobs = {}

        for api_key, db_prefijo in MAP.items():
            api_data = fetch_api_items(session, anio, api_key)
            api_count = len(api_data)
            db_count = conn.execute(
                """
                SELECT COUNT(*)
                FROM documentos
                WHERE tipo='oficio'
                  AND anio=?
                  AND UPPER(COALESCE(subtema,'')) LIKE ?
                """,
                (anio, db_prefijo + "%"),
            ).fetchone()[0]

            api_counts_total += api_count
            db_counts_total += db_count

            estado = "OK" if api_count == db_count else "DIF"
            print(f"  {api_key:6s} | SII={api_count:>4} | BD={db_count:>4} | {estado}")

            year_summary["categorias"][api_key] = {
                "api": api_count,
                "db": db_count,
                "estado": estado,
            }

            for item in api_data:
                blob = (item.get("idBlobArchPublica") or "").strip()
                numero = str(item.get("pubNumOficio", "")).strip()
                titulo = (item.get("pubLegal") or "").replace("\r", " ").replace("\n", " ").strip()
                if not blob:
                    continue
                api_blobs.setdefault(blob, []).append(
                    {
                        "categoria": api_key,
                        "numero": numero,
                        "titulo": titulo,
                    }
                )

        db_blobs = {}
        db_rows = conn.execute(
            """
            SELECT id, numero, anio, titulo, subtema, url_sii, hash_md5, pdf_local
            FROM documentos
            WHERE tipo='oficio' AND anio=?
            """,
            (anio,),
        ).fetchall()
        for row in db_rows:
            blob = extraer_blob(row["url_sii"])
            if blob:
                db_blobs[blob] = dict(row)
        if table_exists(conn, "oficio_fuentes"):
            for row in conn.execute(
                """
                SELECT f.blob_id, f.doc_id, f.numero, f.anio, d.titulo, d.subtema, d.url_sii, d.hash_md5, d.pdf_local
                FROM oficio_fuentes f
                JOIN documentos d ON d.id = f.doc_id
                WHERE f.anio=?
                """,
                (anio,),
            ):
                db_blobs[row["blob_id"]] = {
                    "id": row["doc_id"],
                    "numero": row["numero"],
                    "anio": row["anio"],
                    "titulo": row["titulo"],
                    "subtema": row["subtema"],
                    "url_sii": row["url_sii"],
                    "hash_md5": row["hash_md5"],
                    "pdf_local": row["pdf_local"],
                }

        missing_unique = []
        for blob, items in api_blobs.items():
            if blob not in db_blobs:
                first = items[0]
                missing_unique.append(
                    {
                        "anio": anio,
                        "id_blob": blob,
                        "numero": first["numero"],
                        "categorias": ", ".join(sorted({i["categoria"] for i in items})),
                        "titulo": first["titulo"],
                    }
                )

        unique_estado = "OK" if not missing_unique and len(api_blobs) == len(db_blobs) else "DIF"
        print(
            f"  UNIQUE | SII={len(api_blobs):>4} | BD={len(db_blobs):>4} | "
            f"faltan={len(missing_unique):>3} | {unique_estado}"
        )

        if args.detallar and missing_unique:
            for item in missing_unique:
                print(
                    f"     - faltante blob={item['id_blob']} num={item['numero']} "
                    f"cats={item['categorias']} titulo={item['titulo'][:120]}"
                )

        year_summary["api_unique_blobs"] = len(api_blobs)
        year_summary["db_unique_blobs"] = len(db_blobs)
        year_summary["missing_unique_blobs"] = len(missing_unique)
        year_summary["missing_unique_items"] = missing_unique
        resumen_json["years"].append(year_summary)
        rows_csv.extend(missing_unique)

        total_estado = "OK" if api_counts_total == db_counts_total else "DIF"
        print(f"  TOTAL  | SII={api_counts_total:>4} | BD={db_counts_total:>4} | {total_estado}")

    conn.close()

    with open(detalle_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["anio", "id_blob", "numero", "categorias", "titulo"],
        )
        writer.writeheader()
        writer.writerows(rows_csv)

    with open(resumen_path, "w", encoding="utf-8") as f:
        json.dump(resumen_json, f, ensure_ascii=False, indent=2)

    print("\n" + "-" * 72)
    print(f"Detalle CSV: {detalle_path}")
    print(f"Resumen JSON: {resumen_path}")


if __name__ == "__main__":
    main()
