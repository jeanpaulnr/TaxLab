# -*- coding: utf-8 -*-
"""
Diagnóstico de paridad de jurisprudencia judicial SII vs base local.

Mide:
1. Paridad total por `sii_id` único.
2. Paridad por corte/grupo de instancia.
3. Paridad por año de fecha del pronunciamiento.

Uso:
  python ./scripts/diagnostico_paridad_judicial.py
  python ./scripts/diagnostico_paridad_judicial.py --detallar
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import sys
from collections import Counter
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "sii_normativa.db")
REPORTS = os.path.join(ROOT, "reports")
os.makedirs(REPORTS, exist_ok=True)

sys.path.insert(0, ROOT)

from descargar_jurisprudencia_judicial import buscar_todos_los_pronunciamientos  # noqa: E402


def inferir_corte_desde_instancia(instancia: str | None) -> str:
    value = (instancia or "").casefold()
    if "corte suprema" in value:
        return "Corte Suprema"
    if "tribunal constitucional" in value:
        return "Tribunal Constitucional"
    if "corte de apelaciones" in value:
        return "Corte de Apelaciones"
    if "tribunal tributario y aduanero" in value:
        return "Tribunal Tributario y Aduanero"
    if "tribunal oral" in value and "penal" in value:
        return "Tribunal Oral en lo Penal"
    if "juzgado de garantía" in value or "juzgado de garantia" in value:
        return "Juzgado de Garantía"
    return "Otros"


def normalizar_resumen(value: str | None, maxlen: int = 180) -> str:
    if not value:
        return ""
    text = (
        value.replace("<br/>", " ")
        .replace("<br />", " ")
        .replace("<br>", " ")
        .replace("<p>", " ")
        .replace("</p>", " ")
    )
    text = " ".join(text.split())
    return text[:maxlen]


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def main():
    parser = argparse.ArgumentParser(description="Diagnóstico de paridad de jurisprudencia judicial")
    parser.add_argument("--detallar", action="store_true", help="Mostrar faltantes por sii_id")
    args = parser.parse_args()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    detalle_path = os.path.join(REPORTS, f"paridad_judicial_detalle_{stamp}.csv")
    resumen_path = os.path.join(REPORTS, f"paridad_judicial_resumen_{stamp}.json")

    api_items = buscar_todos_los_pronunciamientos()
    api_count = len(api_items)

    api_ids = set(api_items.keys())
    api_by_corte = Counter()
    api_by_year = Counter()

    detailed_missing = []

    for sii_id, item in api_items.items():
        corte = inferir_corte_desde_instancia(item.get("instancia"))
        api_by_corte[corte] += 1
        fecha = (item.get("fecha") or "").strip()
        if len(fecha) >= 4 and fecha[:4].isdigit():
            api_by_year[int(fecha[:4])] += 1

    conn = get_db()
    db_ids = {
        row["sii_id"]
        for row in conn.execute(
            """
            SELECT j.sii_id
            FROM judicial_docs j
            JOIN documentos d ON d.id = j.doc_id
            WHERE d.tipo='judicial' AND j.sii_id IS NOT NULL
            """
        ).fetchall()
    }

    db_by_corte = Counter()
    for row in conn.execute(
        """
        SELECT COALESCE(NULLIF(TRIM(j.corte), ''), 'Otros') AS corte, COUNT(*) AS total
        FROM judicial_docs j
        JOIN documentos d ON d.id = j.doc_id
        WHERE d.tipo='judicial'
        GROUP BY COALESCE(NULLIF(TRIM(j.corte), ''), 'Otros')
        """
    ):
        db_by_corte[row["corte"]] = row["total"]

    db_by_year = Counter()
    for row in conn.execute(
        """
        SELECT anio, COUNT(*) AS total
        FROM documentos
        WHERE tipo='judicial' AND anio IS NOT NULL
        GROUP BY anio
        """
    ):
        db_by_year[row["anio"]] = row["total"]

    conn.close()

    missing_ids = sorted(api_ids - db_ids)
    extra_ids = sorted(db_ids - api_ids)

    for sii_id in missing_ids:
        item = api_items[sii_id]
        detailed_missing.append(
            {
                "sii_id": sii_id,
                "codigo": item.get("codigo") or "",
                "fecha": item.get("fecha") or "",
                "corte": inferir_corte_desde_instancia(item.get("instancia")),
                "instancia": item.get("instancia") or "",
                "resumen": normalizar_resumen(item.get("resumenInternet") or item.get("resumenIntranet")),
            }
        )

    print("=" * 72)
    print("PARIDAD JURISPRUDENCIA JUDICIAL SII vs BASE")
    print("=" * 72)
    print(f"API SII total:         {api_count}")
    print(f"BD judicial total:     {len(db_ids)}")
    print(f"Faltantes reales:      {len(missing_ids)}")
    print(f"Extras en BD:          {len(extra_ids)}")
    parity = (len(db_ids) / api_count * 100.0) if api_count else 100.0
    print(f"Paridad actual:        {parity:.1f}%")

    print("\nPor corte:")
    ordered_cortes = [
        "Corte Suprema",
        "Tribunal Constitucional",
        "Corte de Apelaciones",
        "Tribunal Tributario y Aduanero",
        "Tribunal Oral en lo Penal",
        "Juzgado de Garantía",
        "Otros",
    ]
    for corte in ordered_cortes:
        api_v = api_by_corte.get(corte, 0)
        db_v = db_by_corte.get(corte, 0)
        estado = "OK" if api_v == db_v else "DIF"
        print(f"  {corte:30s} | SII={api_v:>4} | BD={db_v:>4} | {estado}")

    print("\nPor año:")
    for year in sorted(api_by_year.keys(), reverse=True):
        api_v = api_by_year.get(year, 0)
        db_v = db_by_year.get(year, 0)
        estado = "OK" if api_v == db_v else "DIF"
        print(f"  {year} | SII={api_v:>4} | BD={db_v:>4} | {estado}")

    if args.detallar and detailed_missing:
        print("\nFaltantes detallados:")
        for item in detailed_missing[:200]:
            print(
                f"  - sii_id={item['sii_id']} codigo={item['codigo']} "
                f"fecha={item['fecha']} corte={item['corte']} resumen={item['resumen']}"
            )
        if len(detailed_missing) > 200:
            print(f"  ... y {len(detailed_missing) - 200} más (ver CSV)")

    with open(detalle_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["sii_id", "codigo", "fecha", "corte", "instancia", "resumen"],
        )
        writer.writeheader()
        writer.writerows(detailed_missing)

    resumen = {
        "api_total": api_count,
        "db_total": len(db_ids),
        "missing_total": len(missing_ids),
        "extra_total": len(extra_ids),
        "parity_percent": round(parity, 2),
        "by_corte": {
            corte: {"api": api_by_corte.get(corte, 0), "db": db_by_corte.get(corte, 0)}
            for corte in ordered_cortes
        },
        "by_year": {
            str(year): {"api": api_by_year.get(year, 0), "db": db_by_year.get(year, 0)}
            for year in sorted(api_by_year.keys(), reverse=True)
        },
        "missing_ids": missing_ids,
        "extra_ids": extra_ids,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    with open(resumen_path, "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)

    print("\n" + "-" * 72)
    print(f"Detalle CSV: {detalle_path}")
    print(f"Resumen JSON: {resumen_path}")


if __name__ == "__main__":
    main()
