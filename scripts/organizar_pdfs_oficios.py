import os
import shutil
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "sii_normativa.db"

import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pdf_layout import build_pdf_path


def categoria_oficio(subtema):
    texto = (subtema or "").strip().upper()
    if texto.startswith("IVA "):
        return "iva"
    if texto.startswith("RENTA "):
        return "lir"
    if texto.startswith("OTROS "):
        return "otras_normas"
    if texto.startswith("OTRAS "):
        return "otras_normas"
    return "otras_normas"


def abs_from_rel(rel_path):
    return ROOT / Path((rel_path or "").replace("/", os.sep))


def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    moved = 0
    updated = 0
    missing = 0
    skipped = 0

    rows = conn.execute(
        """
        SELECT id, anio, numero, subtema, pdf_local
        FROM documentos
        WHERE tipo='oficio'
        ORDER BY anio, id
        """
    ).fetchall()

    for row in rows:
        pdf_local = (row["pdf_local"] or "").strip()
        if not pdf_local:
            missing += 1
            continue

        categoria = categoria_oficio(row["subtema"])
        filename = Path(pdf_local).name
        target_abs = Path(build_pdf_path("oficio", row["anio"], filename, categoria=categoria))
        target_rel = os.path.relpath(target_abs, ROOT).replace("\\", "/")

        if pdf_local.replace("\\", "/") == target_rel:
            skipped += 1
            continue

        source_abs = abs_from_rel(pdf_local)
        if source_abs.exists():
            target_abs.parent.mkdir(parents=True, exist_ok=True)
            if target_abs.exists():
                if source_abs.resolve() != target_abs.resolve():
                    source_abs.unlink()
            else:
                shutil.move(str(source_abs), str(target_abs))
                moved += 1
        elif not target_abs.exists():
            missing += 1
            continue

        conn.execute(
            "UPDATE documentos SET pdf_local=? WHERE id=?",
            (target_rel, row["id"]),
        )
        updated += 1

    conn.commit()
    conn.close()

    print("========================================================================")
    print("  REORGANIZACION PDF OFICIOS")
    print("========================================================================")
    print(f"DB:       {DB}")
    print(f"Total:    {len(rows)}")
    print(f"Movidos:  {moved}")
    print(f"Actualiz: {updated}")
    print(f"Skip:     {skipped}")
    print(f"Missing:  {missing}")


if __name__ == "__main__":
    main()
