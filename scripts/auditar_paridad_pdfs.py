import csv
import json
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "sii_normativa.db"
PDF_ROOT = ROOT / "pdfs"
HTML_ROOT = ROOT / "html_historico"
IMG_ROOT = ROOT / "img_historico"
OCR_ROOT = ROOT / "ocr_historico"
DOC_ROOT = ROOT / "doc_historico"
REPORTS_ROOT = ROOT / "reports"

PDF_STATE_OK = "OK_PDF"
PDF_STATE_HTML = "FALTA_PDF_PERO_HAY_HTML"
PDF_STATE_IMG = "FALTA_PDF_PERO_HAY_IMAGEN"
PDF_STATE_OCR = "FALTA_PDF_PERO_HAY_OCR"
PDF_STATE_DOC = "FALTA_PDF_PERO_HAY_DOC"
PDF_STATE_TOTAL = "FALTA_PDF_TOTAL"
PDF_STATE_DUP = "DUPLICADO_PDF"
PDF_STATE_INCONS = "INCONSISTENCIA_IDENTIFICADOR"
PDF_STATE_ORPHAN = "PDF_HUERFANO"

AUX_SUFFIXES = (
    "_original",
    "_limpio",
    "_clean",
    "_raw",
    "_ocr",
    "_scan",
    "_html",
)


def normalize_text(value: Optional[str]) -> str:
    text = (value or "").strip().lower()
    text = (
        text.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ü", "u")
        .replace("ñ", "n")
        .replace("º", "o")
        .replace("°", "o")
        .replace("№", "n")
        .replace("n°", "n")
        .replace("nº", "n")
    )
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def canonical_numero(value: Optional[str]) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    norm = normalize_text(raw)
    if norm.isdigit():
        return str(int(norm))
    match = re.fullmatch(r"0*(\d+)([a-z])", norm)
    if match:
        return f"{int(match.group(1))}{match.group(2)}"
    return norm


def build_key(tipo: str, anio: Optional[int], numero: Optional[str]) -> str:
    return f"{tipo}|{anio or 0}|{canonical_numero(numero)}"


def safe_rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def pdf_is_valid(path: Path) -> Tuple[bool, str]:
    if not path.exists():
        return False, "no_existe"
    if path.stat().st_size < 512:
        return False, "demasiado_pequeno"
    try:
        with path.open("rb") as handle:
            header = handle.read(5)
        if header != b"%PDF-":
            return False, "header_invalido"
    except OSError as exc:
        return False, f"io:{exc.__class__.__name__}"

    try:
        import fitz  # type: ignore

        with fitz.open(path) as doc:
            if doc.page_count <= 0:
                return False, "sin_paginas"
    except ModuleNotFoundError:
        return True, "ok_header_only"
    except Exception as exc:  # pragma: no cover
        return False, f"fitz:{exc.__class__.__name__}"
    return True, "ok"


def historical_stems(anio: Optional[int], numero: Optional[str]) -> List[str]:
    canon = canonical_numero(numero)
    if not canon:
        return []
    stems = {
        f"{anio}_{canon}",
    }
    if canon.isdigit():
        stems.add(f"{anio}_{canon.zfill(2)}")
    return [stem for stem in stems if stem]


def title_tokens(title: Optional[str]) -> List[str]:
    tokens = [token for token in normalize_text(title).split("_") if len(token) >= 4]
    return tokens[:5]


def derive_aux_keys(stem: str) -> List[str]:
    norm = normalize_text(stem)
    keys = {norm}
    work = norm
    changed = True
    while changed:
        changed = False
        for suffix in AUX_SUFFIXES:
            if work.endswith(suffix):
                work = work[: -len(suffix)]
                keys.add(work)
                changed = True
    page_match = re.match(r"(.+)_\d{1,3}$", work)
    if page_match:
        keys.add(page_match.group(1))
    circu_match = re.match(r"(\d{4})_circu(.+)$", work)
    if circu_match:
        anio, numero = circu_match.groups()
        keys.add(f"{anio}_{canonical_numero(numero)}")
        keys.add(f"circu{canonical_numero(numero)}")
        keys.add(canonical_numero(numero))
    num_match = re.match(r"(\d{4})_(\d+[a-z]?)$", work)
    if num_match:
        anio, numero = num_match.groups()
        canon = canonical_numero(numero)
        keys.add(f"{anio}_{canon}")
        keys.add(canon)
    return [key for key in keys if key]


def fetch_documents(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    query = """
        SELECT
            d.id,
            d.tipo,
            d.numero,
            d.anio,
            d.fecha,
            d.titulo,
            d.materia,
            d.url_sii,
            d.referencia,
            d.fuente,
            jd.pdf_local,
            jd.html_local,
            jd.sii_id
        FROM documentos d
        LEFT JOIN judicial_docs jd ON jd.doc_id = d.id
        ORDER BY d.tipo, d.anio, d.numero, d.id
    """
    return list(conn.execute(query))


@dataclass
class ArtifactIndex:
    pdf_by_key: Dict[str, List[Path]]
    pdf_all: List[Path]
    pdf_direct_paths: Dict[str, Path]
    html_by_stem: Dict[str, List[Path]]
    img_by_stem: Dict[str, List[Path]]
    ocr_by_stem: Dict[str, List[Path]]
    doc_by_stem: Dict[str, List[Path]]


def new_index() -> ArtifactIndex:
    return ArtifactIndex(
        pdf_by_key=defaultdict(list),
        pdf_all=[],
        pdf_direct_paths={},
        html_by_stem=defaultdict(list),
        img_by_stem=defaultdict(list),
        ocr_by_stem=defaultdict(list),
        doc_by_stem=defaultdict(list),
    )


def scan_pdf_tree(index: ArtifactIndex) -> None:
    modern_re = re.compile(r"^(circular|oficio|resolucion)_(\d{4})_(.+)\.pdf$", re.I)
    hist_re = re.compile(r"^circular_historica_(\d{4})_(.+?)(?:_(scan|html))?\.pdf$", re.I)
    judicial_re = re.compile(r"^judicial_(\d{4})_.+_id(\d+)\.pdf$", re.I)

    for path in PDF_ROOT.rglob("*"):
        if not path.is_file() or path.suffix.lower() != ".pdf":
            continue

        index.pdf_all.append(path)
        rel = safe_rel(path).replace("\\", "/").lower()
        index.pdf_direct_paths[rel] = path

        name = path.name
        match = modern_re.match(name)
        if match:
            tipo, anio, numero = match.groups()
            index.pdf_by_key[build_key(tipo.lower(), int(anio), numero)].append(path)
            continue

        match = hist_re.match(name)
        if match:
            anio, stem, _mode = match.groups()
            stem_match = re.search(r"circu(.+)$", stem, re.I)
            numero = stem_match.group(1) if stem_match else stem
            index.pdf_by_key[build_key("circular", int(anio), numero)].append(path)
            continue

        match = judicial_re.match(name)
        if match:
            anio, sii_id = match.groups()
            index.pdf_by_key[f"judicial_sii|{anio}|{sii_id}"].append(path)


def index_auxiliary_tree(base: Path, bucket: Dict[str, List[Path]]) -> None:
    if not base.exists():
        return
    for path in base.rglob("*"):
        if not path.is_file():
            continue
        for key in derive_aux_keys(path.stem):
            bucket[key].append(path)


def scan_auxiliary_trees(index: ArtifactIndex) -> None:
    index_auxiliary_tree(HTML_ROOT, index.html_by_stem)
    index_auxiliary_tree(IMG_ROOT, index.img_by_stem)
    index_auxiliary_tree(OCR_ROOT, index.ocr_by_stem)
    index_auxiliary_tree(DOC_ROOT, index.doc_by_stem)


def pick_aux_matches(
    bucket: Dict[str, List[Path]],
    stems: List[str],
    title_fallback: List[str],
) -> List[Path]:
    hits: List[Path] = []
    seen = set()
    for token in stems + title_fallback:
        norm = normalize_text(token)
        for path in bucket.get(norm, []):
            lowered = str(path).lower()
            if lowered not in seen:
                seen.add(lowered)
                hits.append(path)
    return hits


def match_pdf_for_row(row: sqlite3.Row, index: ArtifactIndex) -> Tuple[List[Path], str]:
    if row["tipo"] == "judicial" and (row["pdf_local"] or "").strip():
        rel = row["pdf_local"].replace("\\", "/").lower()
        found = index.pdf_direct_paths.get(rel)
        if found:
            return [found], "pdf_local_judicial"
        return [], "pdf_local_judicial_no_encontrado"

    key = build_key(row["tipo"], row["anio"], row["numero"])
    matches = index.pdf_by_key.get(key, [])
    if matches:
        return matches, "clave_tipo_anio_numero"

    if row["tipo"] == "judicial" and row["sii_id"]:
        matches = index.pdf_by_key.get(f"judicial_sii|{row['anio'] or 0}|{row['sii_id']}", [])
        if matches:
            return matches, "judicial_sii_id"

    return [], "sin_match_pdf"


def classify_row(
    pdf_matches: List[Path],
    pdf_match_kind: str,
    html_matches: List[Path],
    img_matches: List[Path],
    ocr_matches: List[Path],
    doc_matches: List[Path],
) -> Tuple[str, str, str]:
    if len(pdf_matches) > 1:
        return PDF_STATE_DUP, pdf_match_kind, "mas_de_un_pdf_posible"
    if len(pdf_matches) == 1:
        valid, reason = pdf_is_valid(pdf_matches[0])
        if valid:
            return PDF_STATE_OK, pdf_match_kind, ""
        return PDF_STATE_INCONS, pdf_match_kind, f"pdf_invalido:{reason}"
    if html_matches:
        return PDF_STATE_HTML, "html_auxiliar", ""
    if img_matches:
        return PDF_STATE_IMG, "imagen_auxiliar", ""
    if ocr_matches:
        return PDF_STATE_OCR, "ocr_auxiliar", ""
    if doc_matches:
        return PDF_STATE_DOC, "doc_auxiliar", ""
    return PDF_STATE_TOTAL, "sin_fuentes", ""


def build_orphans(index: ArtifactIndex, matched_pdf_paths: List[Path]) -> List[Dict[str, str]]:
    matched = {str(path).lower() for path in matched_pdf_paths}
    rows: List[Dict[str, str]] = []
    for path in index.pdf_all:
        if str(path).lower() in matched:
            continue
        valid, reason = pdf_is_valid(path)
        rows.append(
            {
                "id_db": "",
                "clave_logica": "",
                "tipo": "",
                "numero": "",
                "anio": "",
                "fecha": "",
                "titulo": "",
                "url_fuente": "",
                "fuente_db": "",
                "pdf_encontrado": "1",
                "ruta_pdf": safe_rel(path),
                "pdf_valido": "1" if valid else "0",
                "pdf_motivo": reason,
                "html_encontrado": "0",
                "ruta_html": "",
                "imagen_encontrada": "0",
                "ruta_imagen": "",
                "ocr_encontrado": "0",
                "ruta_ocr": "",
                "doc_encontrado": "0",
                "ruta_doc": "",
                "estado_paridad": PDF_STATE_ORPHAN,
                "matching_usado": "ninguno",
                "observaciones": "",
            }
        )
    return rows


def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    if not DB_PATH.exists():
        raise SystemExit(f"No existe la base SQLite: {DB_PATH}")

    REPORTS_ROOT.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    rows = fetch_documents(conn)
    conn.close()

    index = new_index()
    scan_pdf_tree(index)
    scan_auxiliary_trees(index)

    detail_rows: List[Dict[str, str]] = []
    candidates_rows: List[Dict[str, str]] = []
    matched_pdf_paths: List[Path] = []

    for row in rows:
        pdf_matches, pdf_match_kind = match_pdf_for_row(row, index)
        stems = historical_stems(row["anio"], row["numero"])
        title_fallback = title_tokens(row["titulo"])
        html_matches = pick_aux_matches(index.html_by_stem, stems, title_fallback)
        img_matches = pick_aux_matches(index.img_by_stem, stems, title_fallback)
        ocr_matches = pick_aux_matches(index.ocr_by_stem, stems, title_fallback)
        doc_matches = pick_aux_matches(index.doc_by_stem, stems, title_fallback)

        state, matching_used, observaciones = classify_row(
            pdf_matches,
            pdf_match_kind,
            html_matches,
            img_matches,
            ocr_matches,
            doc_matches,
        )

        pdf_path = pdf_matches[0] if len(pdf_matches) == 1 else None
        if pdf_path:
            matched_pdf_paths.append(pdf_path)
        pdf_valid, pdf_reason = pdf_is_valid(pdf_path) if pdf_path else (False, "")

        detail = {
            "id_db": str(row["id"]),
            "clave_logica": build_key(row["tipo"], row["anio"], row["numero"]),
            "tipo": row["tipo"] or "",
            "numero": row["numero"] or "",
            "anio": str(row["anio"] or ""),
            "fecha": row["fecha"] or "",
            "titulo": row["titulo"] or "",
            "url_fuente": row["url_sii"] or "",
            "fuente_db": row["fuente"] or "",
            "pdf_encontrado": "1" if pdf_matches else "0",
            "ruta_pdf": safe_rel(pdf_path) if pdf_path else "",
            "pdf_valido": "1" if pdf_valid else "0",
            "pdf_motivo": pdf_reason,
            "html_encontrado": "1" if html_matches else "0",
            "ruta_html": safe_rel(html_matches[0]) if html_matches else "",
            "imagen_encontrada": "1" if img_matches else "0",
            "ruta_imagen": safe_rel(img_matches[0]) if img_matches else "",
            "ocr_encontrado": "1" if ocr_matches else "0",
            "ruta_ocr": safe_rel(ocr_matches[0]) if ocr_matches else "",
            "doc_encontrado": "1" if doc_matches else "0",
            "ruta_doc": safe_rel(doc_matches[0]) if doc_matches else "",
            "estado_paridad": state,
            "matching_usado": matching_used,
            "observaciones": observaciones,
        }
        detail_rows.append(detail)

        if state in {PDF_STATE_HTML, PDF_STATE_IMG, PDF_STATE_OCR, PDF_STATE_DOC}:
            candidates_rows.append(detail)

    orphan_rows = build_orphans(index, matched_pdf_paths)
    all_rows = detail_rows + orphan_rows

    counts = Counter(row["estado_paridad"] for row in all_rows)
    total_docs = len(detail_rows)
    ok_pdf = counts[PDF_STATE_OK]
    total_no_pdf = total_docs - ok_pdf
    parity = round((ok_pdf / total_docs) * 100, 2) if total_docs else 0.0

    fieldnames = [
        "id_db",
        "clave_logica",
        "tipo",
        "numero",
        "anio",
        "fecha",
        "titulo",
        "url_fuente",
        "fuente_db",
        "pdf_encontrado",
        "ruta_pdf",
        "pdf_valido",
        "pdf_motivo",
        "html_encontrado",
        "ruta_html",
        "imagen_encontrada",
        "ruta_imagen",
        "ocr_encontrado",
        "ruta_ocr",
        "doc_encontrado",
        "ruta_doc",
        "estado_paridad",
        "matching_usado",
        "observaciones",
    ]

    detail_path = REPORTS_ROOT / "paridad_pdfs_detalle.csv"
    orphan_path = REPORTS_ROOT / "paridad_pdfs_huerfanos.csv"
    candidates_path = REPORTS_ROOT / "candidatos_conversion_a_pdf.csv"
    summary_path = REPORTS_ROOT / "paridad_pdfs_resumen.json"

    write_csv(detail_path, fieldnames, detail_rows)
    write_csv(orphan_path, fieldnames, orphan_rows)
    write_csv(candidates_path, fieldnames, candidates_rows)

    summary = {
        "db_path": safe_rel(DB_PATH),
        "pdf_root": safe_rel(PDF_ROOT),
        "html_root": safe_rel(HTML_ROOT),
        "img_root": safe_rel(IMG_ROOT),
        "ocr_root": safe_rel(OCR_ROOT),
        "doc_root": safe_rel(DOC_ROOT),
        "total_documentos_bd": total_docs,
        "total_pdfs_en_disco": len(index.pdf_all),
        "total_con_pdf_valido": ok_pdf,
        "total_sin_pdf": total_no_pdf,
        "total_con_html_sin_pdf": counts[PDF_STATE_HTML],
        "total_con_imagen_sin_pdf": counts[PDF_STATE_IMG],
        "total_con_ocr_sin_pdf": counts[PDF_STATE_OCR],
        "total_con_doc_sin_pdf": counts[PDF_STATE_DOC],
        "total_sin_fuente_recuperable": counts[PDF_STATE_TOTAL],
        "total_pdf_huerfano": counts[PDF_STATE_ORPHAN],
        "total_pdf_duplicado": counts[PDF_STATE_DUP],
        "total_inconsistencia": counts[PDF_STATE_INCONS],
        "porcentaje_paridad_actual": parity,
        "matching_strategy": {
            "principal": "tipo+anio+numero",
            "judicial": "judicial_docs.pdf_local y fallback sii_id",
            "historico": "stems por anio/numero/titulo sobre html/img/ocr/doc",
            "regla": "No se asocian coincidencias ambiguas como OK_PDF.",
        },
    }

    with summary_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)

    print("=" * 72)
    print("  AUDITORIA DE PARIDAD PDF")
    print("=" * 72)
    print(f"Base SQLite:         {DB_PATH}")
    print(f"PDF root:            {PDF_ROOT}")
    print(f"Documentos BD:       {total_docs}")
    print(f"PDFs en disco:       {len(index.pdf_all)}")
    print(f"Con PDF valido:      {ok_pdf}")
    print(f"Sin PDF:             {total_no_pdf}")
    print(f"Sin PDF pero HTML:   {counts[PDF_STATE_HTML]}")
    print(f"Sin PDF pero imagen: {counts[PDF_STATE_IMG]}")
    print(f"Sin PDF pero OCR:    {counts[PDF_STATE_OCR]}")
    print(f"Sin PDF pero DOC:    {counts[PDF_STATE_DOC]}")
    print(f"Sin PDF total:       {counts[PDF_STATE_TOTAL]}")
    print(f"PDFs huerfanos:      {counts[PDF_STATE_ORPHAN]}")
    print(f"PDFs duplicados:     {counts[PDF_STATE_DUP]}")
    print(f"Inconsistencias:     {counts[PDF_STATE_INCONS]}")
    print(f"Paridad actual:      {parity}%")
    print("-" * 72)
    print(f"Detalle CSV:         {detail_path}")
    print(f"Huerfanos CSV:       {orphan_path}")
    print(f"Candidatos CSV:      {candidates_path}")
    print(f"Resumen JSON:        {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


