"""
SII Normativa - Ingestor de Jurisprudencia Judicial
==================================================

Objetivo:
- Incorporar la jurisprudencia judicial del SII al corpus local.
- No perder documentos: usa consulta global y validacion por arbol.
- Guardar cada causa completa en la base y generar PDF/HTML local.

Uso:
  python descargar_jurisprudencia_judicial.py
  python descargar_jurisprudencia_judicial.py --desde 2015 --hasta 2026
  python descargar_jurisprudencia_judicial.py --ids 2623
  python descargar_jurisprudencia_judicial.py --sin-verificacion
"""

import argparse
import hashlib
import html
import json
import os
import re
import sys
import textwrap
import time
import uuid
from datetime import date, datetime

import fitz

BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE, "scraper"))

from engine import (  # noqa: E402
    PDF_DIR,
    SESSION,
    detectar_articulos,
    detectar_leyes,
    doc_existe_hash,
    get_db,
    guardar_documento,
    log,
    log_scraper,
)

APP_BASE = "https://www4.sii.cl/acjui"
API_BASE = APP_BASE + "/services/data/internetService"
DETALLE_WEB = APP_BASE + "/internet/#/pronunciamiento/{id}"
NAMESPACE = "cl.sii.sdi.lob.juridica.acj.data.impl.InternetApplicationService"
TIPO_INSTANCIA_JUDICIAL = 1
CONVERSATION_ID = "####"
REPORT_DIR = os.path.join(BASE, "logs")
JUDICIAL_PDF_DIR = os.path.join(PDF_DIR, "judicial")
JUDICIAL_HTML_DIR = os.path.join(PDF_DIR, "judicial_html")

os.makedirs(REPORT_DIR, exist_ok=True)
os.makedirs(JUDICIAL_PDF_DIR, exist_ok=True)
os.makedirs(JUDICIAL_HTML_DIR, exist_ok=True)

CORTE_POR_GRUPO = {
    1: "Corte Suprema",
    2: "Tribunal Constitucional",
    3: "Corte de Apelaciones",
    4: "Tribunal Tributario y Aduanero",
    5: "Tribunal Oral en lo Penal",
    6: "Juzgado de Garantia",
    7: "Otros",
}


def create_uuid():
    return str(uuid.uuid4())


def post_api(path, namespace, data=None, timeout=40):
    payload = {
        "metaData": {
            "namespace": f"{NAMESPACE}/{namespace}",
            "conversationId": CONVERSATION_ID,
            "transactionId": create_uuid(),
            "page": None,
        },
        "data": data or {},
    }
    response = SESSION.post(
        API_BASE + path,
        json=payload,
        headers={
            "Content-Type": "application/json",
            "Origin": "https://www4.sii.cl",
            "Referer": APP_BASE + "/internet/",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    body = response.json()
    errors = ((body.get("metaData") or {}).get("errors")) or []
    if errors:
        raise RuntimeError(str(errors))
    return body.get("data")


def html_to_text(value):
    if not value:
        return ""
    text = value.replace("<br/>", "\n").replace("<br />", "\n").replace("<br>", "\n")
    text = re.sub(r"</p\s*>", "\n\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def unique_list(values):
    seen = set()
    out = []
    for value in values:
        item = normalize(value)
        if not item:
            continue
        key = item.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def safe_slug(value, default="sin_codigo", maxlen=90):
    value = normalize(value)
    value = re.sub(r"[^0-9A-Za-z._-]+", "_", value)
    value = value.strip("._-")
    return (value or default)[:maxlen]


def relative_asset_path(path):
    return os.path.relpath(path, BASE).replace(os.sep, "/")


def ensure_judicial_schema():
    conn = get_db()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS judicial_docs (
                doc_id       INTEGER PRIMARY KEY REFERENCES documentos(id) ON DELETE CASCADE,
                sii_id       INTEGER UNIQUE,
                tipo_codigo  TEXT,
                corte        TEXT,
                tribunal     TEXT,
                pdf_local    TEXT,
                html_local   TEXT
            );
            CREATE TABLE IF NOT EXISTS judicial_relaciones (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_id           INTEGER REFERENCES documentos(id) ON DELETE CASCADE,
                cuerpo_normativo TEXT,
                articulo         TEXT,
                nota             TEXT,
                UNIQUE(doc_id, cuerpo_normativo, articulo, nota)
            );
            CREATE INDEX IF NOT EXISTS idx_judicial_corte    ON judicial_docs(corte);
            CREATE INDEX IF NOT EXISTS idx_judicial_tribunal ON judicial_docs(tribunal);
            CREATE INDEX IF NOT EXISTS idx_judicial_cuerpo   ON judicial_relaciones(cuerpo_normativo);
            CREATE INDEX IF NOT EXISTS idx_judicial_art      ON judicial_relaciones(articulo);
            """
        )
        conn.commit()
    finally:
        conn.close()


def inferir_corte(detalle):
    grupo = detalle.get("grupoInstancia") or {}
    grupo_id = grupo.get("id")
    if grupo_id in CORTE_POR_GRUPO:
        return CORTE_POR_GRUPO[grupo_id]

    instancia = normalize((detalle.get("instancia") or {}).get("nombre"))
    instancia_l = instancia.casefold()
    if "corte suprema" in instancia_l:
        return "Corte Suprema"
    if "tribunal constitucional" in instancia_l:
        return "Tribunal Constitucional"
    if "corte de apelaciones" in instancia_l:
        return "Corte de Apelaciones"
    if "tribunal tributario y aduanero" in instancia_l:
        return "Tribunal Tributario y Aduanero"
    if "tribunal oral" in instancia_l and "penal" in instancia_l:
        return "Tribunal Oral en lo Penal"
    if "juzgado de garantia" in instancia_l or "juzgado de garantía" in instancia_l:
        return "Juzgado de Garantia"
    return "Otros"


def build_search_form():
    return {
        "text": None,
        "tipoInstanciaId": TIPO_INSTANCIA_JUDICIAL,
        "grupoInstanciaId": None,
        "tipoCodigoId": None,
        "codigo": None,
        "ruc": None,
        "instanciaId": None,
        "tipoPronunciamientoId": None,
        "cuerpoNormativoId": None,
        "articulosIds": [],
        "reemplazos": [],
        "fechaDesde": None,
        "fechaHasta": None,
    }


def buscar_todos_los_pronunciamientos():
    data = post_api(
        "/find-pronunciamientos",
        "findPronunciamientos",
        build_search_form(),
        timeout=90,
    )
    if not data:
        return {}
    return {int(k): v for k, v in data.items()}


def obtener_resumen_nivel_1():
    return post_api(
        "/pronunciamientos-por-cuerpo-normativo-y-grupo-instancia",
        "getPronunciamientosPorCuerpoNormativoYGrupoInstancia",
        {"id": TIPO_INSTANCIA_JUDICIAL},
    )


def obtener_resumen_nivel_2(grupo_instancia_id, cuerpo_normativo_id):
    return post_api(
        "/pronunciamientos-por-articulo-y-grupo-instancia",
        "getPronunciamientosPorArticuloYGrupoInstancia",
        {
            "grupoInstancia": {"id": grupo_instancia_id},
            "cuerpoNormativo": {"id": cuerpo_normativo_id},
        },
    )


def obtener_resultados_explorador(grupo_instancia_id, articulo_id):
    return post_api(
        "/resultados-explorador",
        "getResultadosExplorador",
        {
            "articulo": {"id": articulo_id},
            "grupoInstancia": {"id": grupo_instancia_id},
        },
        timeout=60,
    )


def obtener_detalle(pronunciamiento_id):
    return post_api(
        "/pronunciamientos/get-full",
        "getFullPronunciamiento",
        {"id": pronunciamiento_id},
        timeout=60,
    )


def recolectar_ids_desde_arbol(delay=0.0):
    ids = set()
    resumen = obtener_resumen_nivel_1() or {}
    body = resumen.get("body") or []

    for normativa in body:
        cuerpo_id = normativa.get("id")
        nombre = normalize(normativa.get("nombre"))
        cells = normativa.get("cells") or {}
        grupos = sorted(int(gid) for gid, total in cells.items() if int(total or 0) > 0)

        for grupo_id in grupos:
            try:
                nivel2 = obtener_resumen_nivel_2(grupo_id, cuerpo_id) or {}
            except Exception as exc:
                log.warning(f"[judicial] nivel2 fallo normativa={cuerpo_id} grupo={grupo_id}: {exc}")
                continue

            for bloque in nivel2.get("body") or []:
                for articulo in bloque.get("articulos") or []:
                    articulo_id = articulo.get("idArticulo")
                    if not articulo_id:
                        continue
                    try:
                        resultados = obtener_resultados_explorador(grupo_id, articulo_id) or {}
                    except Exception as exc:
                        log.warning(
                            f"[judicial] explorador fallo normativa={cuerpo_id} grupo={grupo_id} articulo={articulo_id}: {exc}"
                        )
                        continue

                    ids.update(int(k) for k in resultados.keys())
                    if delay > 0:
                        time.sleep(delay)

            log.info(f"[judicial] arbol: normativa={nombre or cuerpo_id} grupo={grupo_id} ids={len(ids)}")

    return ids


def extraer_relaciones(detalle, texto_plano):
    leyes_textuales = []
    leyes_siglas = []
    articulos = []
    relaciones_oficiales = []
    seen_rel = set()

    for item in detalle.get("pronunciamientosArticulos") or []:
        articulo = item.get("articulo") or {}
        cuerpo = ((articulo.get("tituloBO") or {}).get("cuerpoNormativo") or {})
        ley_nombre = normalize(cuerpo.get("nombre"))
        if ley_nombre:
            leyes_textuales.append(ley_nombre)

        nombre_art = normalize(articulo.get("nombre"))
        nota = normalize(item.get("nota"))
        if nombre_art and nota:
            articulos.append(f"{nombre_art} ({nota})")
        elif nombre_art:
            articulos.append(nombre_art)

        rel_key = (ley_nombre.casefold(), nombre_art.casefold(), nota.casefold())
        if rel_key not in seen_rel and any(rel_key):
            seen_rel.add(rel_key)
            relaciones_oficiales.append(
                {
                    "cuerpo_normativo": ley_nombre,
                    "articulo": nombre_art,
                    "nota": nota,
                }
            )

    leyes_siglas.extend(detectar_leyes(texto_plano))
    articulos.extend(detectar_articulos(texto_plano))

    return {
        "leyes_textuales": unique_list(leyes_textuales),
        "leyes_siglas": unique_list(leyes_siglas),
        "articulos": unique_list(articulos)[:30],
        "relaciones_oficiales": relaciones_oficiales,
    }


def construir_titulo(detalle, texto_resumen, texto_extracto):
    codigo_tipo = normalize((detalle.get("tipoCodigo") or {}).get("nombre"))
    codigo = normalize(detalle.get("codigoPronunciamiento"))
    partes = normalize(detalle.get("partes"))
    instancia = normalize((detalle.get("instancia") or {}).get("nombre"))

    if codigo_tipo and codigo and partes:
        return f"{codigo_tipo} {codigo} - {partes}"[:500]
    if codigo_tipo and codigo:
        return f"{codigo_tipo} {codigo}"[:500]
    if codigo and instancia:
        return f"Rol {codigo} - {instancia}"[:500]
    if texto_resumen:
        return texto_resumen.splitlines()[0][:500]
    if texto_extracto:
        return texto_extracto.splitlines()[0][:500]
    return f"Jurisprudencia Judicial SII {detalle.get('id')}"[:500]


def construir_referencia(detalle):
    tipo_pron = normalize((detalle.get("tipoPronunciamiento") or {}).get("nombre"))
    instancia = normalize((detalle.get("instancia") or {}).get("nombre"))
    tipo_codigo = normalize((detalle.get("tipoCodigo") or {}).get("nombre"))
    codigo = normalize(detalle.get("codigoPronunciamiento"))
    fecha = normalize(detalle.get("fecha"))
    partes = normalize(detalle.get("partes"))

    piezas = ["Jurisprudencia Judicial SII"]
    if instancia:
        piezas.append(instancia)
    if tipo_pron:
        piezas.append(tipo_pron)
    if tipo_codigo and codigo:
        piezas.append(f"{tipo_codigo} {codigo}")
    elif codigo:
        piezas.append(f"Rol {codigo}")
    if fecha:
        piezas.append(fecha)
    if partes:
        piezas.append(partes)
    return " | ".join(piezas)[:500]


def construir_palabras_clave(detalle, relaciones, texto_resumen, texto_extracto):
    valores = [
        detalle.get("codigoPronunciamiento"),
        detalle.get("partes"),
        detalle.get("ruc"),
        (detalle.get("instancia") or {}).get("nombre"),
        (detalle.get("tipoPronunciamiento") or {}).get("nombre"),
        (detalle.get("tipoCodigo") or {}).get("nombre"),
        detalle.get("resultado"),
        detalle.get("decision"),
        texto_resumen,
        texto_extracto,
    ]
    valores.extend(relaciones["leyes_textuales"])
    valores.extend(relaciones["leyes_siglas"])
    valores.extend(relaciones["articulos"][:10])
    return " | ".join(unique_list(valores))[:2000] or None


def construir_articulos_lineas(detalle):
    por_ley = {}
    for item in detalle.get("pronunciamientosArticulos") or []:
        articulo = item.get("articulo") or {}
        cuerpo = ((articulo.get("tituloBO") or {}).get("cuerpoNormativo") or {})
        ley = normalize(cuerpo.get("nombre")) or "Sin ley"
        art = normalize(articulo.get("nombre"))
        nota = normalize(item.get("nota"))
        if art and nota:
            art = f"{art} {nota}".strip()
        if art:
            por_ley.setdefault(ley, []).append(art)

    lineas = []
    for ley, arts in por_ley.items():
        lineas.append(f"- {ley}: {' | '.join(unique_list(arts))}")
    return lineas or ["- Sin articulos asociados"]


def construir_contenido_struct(detalle, texto_sentencia, texto_extracto, texto_resumen, relaciones):
    bloques = [
        "JURISPRUDENCIA JUDICIAL SII",
        "",
        f"ID SII: {detalle.get('id')}",
        f"Tipo Codigo: {normalize((detalle.get('tipoCodigo') or {}).get('nombre')) or 'Sin dato'}",
        f"Codigo/Rol: {normalize(detalle.get('codigoPronunciamiento')) or 'Sin dato'}",
        f"Fecha: {normalize(detalle.get('fecha')) or 'Sin dato'}",
        f"Instancia: {normalize((detalle.get('instancia') or {}).get('nombre')) or 'Sin dato'}",
        f"Tipo de pronunciamiento: {normalize((detalle.get('tipoPronunciamiento') or {}).get('nombre')) or 'Sin dato'}",
        f"Partes: {normalize(detalle.get('partes')) or 'Sin dato'}",
        f"RUC: {normalize(detalle.get('ruc')) or 'Sin dato'}",
        f"Resultado: {normalize(detalle.get('resultado')) or 'Sin dato'}",
        f"Decision: {normalize(detalle.get('decision')) or 'Sin dato'}",
        f"Fuente SII: {DETALLE_WEB.format(id=detalle.get('id'))}",
        "",
        "LEYES CITADAS",
        ", ".join(relaciones["leyes_textuales"] + relaciones["leyes_siglas"]) or "Sin dato",
        "",
        "ARTICULOS",
        *construir_articulos_lineas(detalle),
        "",
        "EXTRACTO",
        texto_extracto or "Sin dato",
        "",
        "DESCRIPTORES",
        texto_resumen or "Sin dato",
        "",
        "PRONUNCIAMIENTO COMPLETO",
        texto_sentencia or texto_extracto or texto_resumen or "Sin dato",
    ]
    return "\n".join(bloques).strip()


def build_file_stem(detalle, anio):
    codigo_tipo = safe_slug((detalle.get("tipoCodigo") or {}).get("nombre"), default="DOC")
    codigo = safe_slug(detalle.get("codigoPronunciamiento"), default=str(detalle.get("id")))
    return f"judicial_{anio}_{codigo_tipo}_{codigo}_id{detalle.get('id')}"


def guardar_html_fuente(detalle, html_pronunciamiento, anio):
    folder = os.path.join(JUDICIAL_HTML_DIR, str(anio))
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, build_file_stem(detalle, anio) + ".html")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(html_pronunciamiento or "")
    return path


def crear_pdf_judicial(detalle, contenido_struct, anio):
    folder = os.path.join(JUDICIAL_PDF_DIR, str(anio))
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, build_file_stem(detalle, anio) + ".pdf")

    doc = fitz.open()
    page_width = 595
    page_height = 842
    margin = 48
    line_height = 15
    page = doc.new_page(width=page_width, height=page_height)
    y = margin
    max_y = page_height - margin
    wrap_width = 96

    def new_page():
        nonlocal page, y
        page = doc.new_page(width=page_width, height=page_height)
        y = margin

    def ensure_space(lines_needed=1):
        nonlocal y
        if y + (lines_needed * line_height) > max_y:
            new_page()

    def write_line(text, fontsize=11, gap=4):
        nonlocal y
        ensure_space()
        page.insert_text((margin, y), text, fontname="helv", fontsize=fontsize)
        y += fontsize + gap

    titulo = construir_titulo(detalle, "", "")
    referencia = construir_referencia(detalle)
    write_line(titulo, fontsize=18, gap=10)
    write_line(referencia, fontsize=10, gap=12)

    for raw_paragraph in contenido_struct.split("\n"):
        paragraph = raw_paragraph.rstrip()
        if not paragraph:
            y += 6
            continue

        is_heading = paragraph.isupper() and len(paragraph) < 80
        fontsize = 13 if is_heading else 11
        gap = 6 if is_heading else 4
        width = 88 if is_heading else wrap_width

        wrapped = textwrap.wrap(
            paragraph,
            width=width,
            break_long_words=False,
            break_on_hyphens=False,
        ) or [""]

        ensure_space(len(wrapped) + 1)
        for line in wrapped:
            page.insert_text((margin, y), line, fontname="helv", fontsize=fontsize)
            y += fontsize + gap

    pages = len(doc)
    doc.save(path)
    doc.close()
    return path, pages


def actualizar_campos_tecnicos(doc_id, contenido_struct, paginas):
    conn = get_db()
    try:
        conn.execute(
            "UPDATE documentos SET chars_texto=?, paginas=? WHERE id=?",
            (len(contenido_struct), paginas, doc_id),
        )
        conn.commit()
    finally:
        conn.close()


def actualizar_documento_base(doc_id, doc_data, contenido_struct, paginas):
    conn = get_db()
    try:
        conn.execute(
            """
            UPDATE documentos
            SET numero=?, anio=?, fecha=?, titulo=?, materia=?, subtema=?,
                contenido=?, resumen=?, url_sii=?, referencia=?, palabras_clave=?,
                leyes_citadas=?, articulos_clave=?, paginas=?, chars_texto=?, fuente=?
            WHERE id=?
            """,
            (
                doc_data["numero"],
                doc_data["anio"],
                doc_data["fecha"],
                doc_data["titulo"],
                doc_data["materia"],
                doc_data["subtema"],
                doc_data["contenido"],
                doc_data["resumen"],
                doc_data["url_sii"],
                doc_data["referencia"],
                doc_data["palabras_clave"],
                doc_data["leyes_citadas"],
                doc_data["articulos_clave"],
                paginas,
                len(contenido_struct),
                doc_data["fuente"],
                doc_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def guardar_metadata_judicial(doc_id, detalle, relaciones, pdf_path, html_path):
    conn = get_db()
    try:
        conn.execute(
            """
            INSERT INTO judicial_docs(doc_id, sii_id, tipo_codigo, corte, tribunal, pdf_local, html_local)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(doc_id) DO UPDATE SET
                sii_id=excluded.sii_id,
                tipo_codigo=excluded.tipo_codigo,
                corte=excluded.corte,
                tribunal=excluded.tribunal,
                pdf_local=excluded.pdf_local,
                html_local=excluded.html_local
            """,
            (
                doc_id,
                int(detalle["id"]),
                normalize((detalle.get("tipoCodigo") or {}).get("nombre")) or None,
                inferir_corte(detalle),
                normalize((detalle.get("instancia") or {}).get("nombre")) or None,
                relative_asset_path(pdf_path),
                relative_asset_path(html_path),
            ),
        )
        conn.execute("DELETE FROM judicial_relaciones WHERE doc_id=?", (doc_id,))
        for rel in relaciones["relaciones_oficiales"]:
            conn.execute(
                """
                INSERT OR IGNORE INTO judicial_relaciones(doc_id, cuerpo_normativo, articulo, nota)
                VALUES(?,?,?,?)
                """,
                (
                    doc_id,
                    rel.get("cuerpo_normativo") or None,
                    rel.get("articulo") or None,
                    rel.get("nota") or None,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def guardar_pronunciamiento(detalle):
    contenido = detalle.get("contenido") or {}
    fecha = normalize(detalle.get("fecha"))[:10] or "1900-01-01"
    anio = int(fecha[:4]) if re.match(r"^\d{4}-\d{2}-\d{2}$", fecha) else 1900
    pron_id = int(detalle["id"])

    html_sentencia = (
        contenido.get("sentenciaInternet")
        or contenido.get("sentenciaRaw")
        or contenido.get("sentenciaIntranet")
        or ""
    )
    texto_sentencia = html_to_text(html_sentencia)
    texto_extracto = html_to_text(
        contenido.get("extractoInternet")
        or contenido.get("extractoRaw")
        or contenido.get("extractoIntranet")
    )
    texto_resumen = html_to_text(
        contenido.get("resumenInternet")
        or contenido.get("resumenRaw")
        or contenido.get("resumenIntranet")
    )

    texto_base = texto_sentencia or texto_extracto or texto_resumen
    if not texto_base:
        raise RuntimeError(f"Pronunciamiento sin contenido util: {pron_id}")

    relaciones = extraer_relaciones(detalle, texto_base)
    contenido_struct = construir_contenido_struct(
        detalle,
        texto_sentencia,
        texto_extracto,
        texto_resumen,
        relaciones,
    )

    html_path = guardar_html_fuente(detalle, html_sentencia, anio)
    pdf_path, paginas = crear_pdf_judicial(detalle, contenido_struct, anio)

    hash_md5 = hashlib.md5(f"judicial:{pron_id}".encode("utf-8")).hexdigest()
    ya_existia = doc_existe_hash(hash_md5)

    tipo_pron = normalize((detalle.get("tipoPronunciamiento") or {}).get("nombre"))
    instancia = normalize((detalle.get("instancia") or {}).get("nombre"))
    resumen_guardado = texto_extracto or texto_resumen or texto_base[:3000]

    doc_data = {
        "hash_md5": hash_md5,
        "tipo": "judicial",
        "numero": (normalize(detalle.get("codigoPronunciamiento")) or str(pron_id))[:100],
        "anio": anio,
        "fecha": fecha,
        "titulo": construir_titulo(detalle, texto_resumen, texto_extracto),
        "materia": (tipo_pron or "Jurisprudencia Judicial")[:200],
        "subtema": (instancia or "SII Judicial")[:200],
        "contenido": contenido_struct,
        "resumen": resumen_guardado,
        "url_sii": DETALLE_WEB.format(id=pron_id),
        "referencia": construir_referencia(detalle),
        "palabras_clave": construir_palabras_clave(detalle, relaciones, texto_resumen, texto_extracto),
        "leyes_citadas": json.dumps(relaciones["leyes_siglas"], ensure_ascii=False),
        "articulos_clave": json.dumps(relaciones["articulos"], ensure_ascii=False),
        "fuente": "sii_judicial",
    }

    doc_id = guardar_documento(doc_data)
    if not doc_id:
        raise RuntimeError(f"No pude guardar el pronunciamiento {pron_id} en la base")

    actualizar_documento_base(doc_id, doc_data, contenido_struct, paginas)
    guardar_metadata_judicial(doc_id, detalle, relaciones, pdf_path, html_path)
    actualizar_campos_tecnicos(doc_id, contenido_struct, paginas)
    return doc_id, ya_existia, pdf_path, html_path


def filtrar_por_anio(resultados, desde, hasta):
    filtrados = {}
    for pron_id, item in resultados.items():
        fecha = normalize(item.get("fecha"))[:10]
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", fecha):
            continue
        anio = int(fecha[:4])
        if desde <= anio <= hasta:
            filtrados[pron_id] = item
    return filtrados


def extraer_anio_detalle(pron_id):
    try:
        detalle = obtener_detalle(pron_id)
        fecha = normalize(detalle.get("fecha"))[:10]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", fecha):
            return int(fecha[:4])
    except Exception as exc:
        log.warning(f"[judicial] no pude leer anio de {pron_id}: {exc}")
    return 0


def guardar_reporte(data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(REPORT_DIR, f"reporte_judicial_{timestamp}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)
    return path


def parse_ids(ids_arg):
    ids = []
    for item in str(ids_arg or "").split(","):
        item = item.strip()
        if not item:
            continue
        ids.append(int(item))
    return list(dict.fromkeys(ids))


def resolver_ids_objetivo(desde, hasta, verificar_arbol, ids_explicitos, delay):
    if ids_explicitos:
        return {
            "ids_globales": set(ids_explicitos),
            "ids_arbol": set(),
            "ids_finales": ids_explicitos,
            "faltantes": [],
            "modo": "ids",
        }

    resultados = buscar_todos_los_pronunciamientos()
    resultados = filtrar_por_anio(resultados, desde, hasta)
    ids_globales = set(resultados.keys())

    ids_arbol = set()
    if verificar_arbol:
        ids_arbol = recolectar_ids_desde_arbol(delay=delay)
        ids_arbol = {
            pron_id
            for pron_id in ids_arbol
            if pron_id in resultados or (desde <= extraer_anio_detalle(pron_id) <= hasta)
        }

    ids_finales = sorted(ids_globales | ids_arbol)
    faltantes = sorted(ids_arbol - ids_globales)
    return {
        "ids_globales": ids_globales,
        "ids_arbol": ids_arbol,
        "ids_finales": ids_finales,
        "faltantes": faltantes,
        "modo": "masivo",
    }


def descargar_jurisprudencia_judicial(desde, hasta, verificar_arbol=True, delay=0.0, max_docs=None, ids_explicitos=None):
    ensure_judicial_schema()

    print("\n" + "=" * 72)
    print("  INGESTA DE JURISPRUDENCIA JUDICIAL SII")
    print(f"  Anios: {desde} a {hasta}")
    print(f"  Verificacion arbol: {'SI' if verificar_arbol else 'NO'}")
    if ids_explicitos:
        print(f"  IDs puntuales: {', '.join(str(x) for x in ids_explicitos)}")
    print("=" * 72)

    if ids_explicitos:
        print("[1/4] Modo validacion por IDs...")
    else:
        print("[1/4] Consulta global del buscador judicial...")

    info_ids = resolver_ids_objetivo(desde, hasta, verificar_arbol, ids_explicitos, delay)
    ids_globales = info_ids["ids_globales"]
    ids_arbol = info_ids["ids_arbol"]
    ids_finales = info_ids["ids_finales"]
    faltantes = info_ids["faltantes"]

    if not ids_explicitos:
        print(f"      IDs por buscador: {len(ids_globales)}")
        if verificar_arbol:
            print("[2/4] Verificando por arbol oficial del SII...")
            print(f"      IDs por arbol: {len(ids_arbol)}")
        else:
            print("[2/4] Verificacion por arbol desactivada.")
    else:
        print(f"      IDs solicitados: {len(ids_finales)}")
        print("[2/4] Saltando buscador masivo y arbol por prueba puntual.")

    print("[3/4] Descargando detalle completo, generando PDF y guardando en base...")
    print(f"      IDs finales: {len(ids_finales)}")
    if faltantes:
        print(f"      IDs extra detectados por arbol: {len(faltantes)}")

    nuevos = 0
    existentes = 0
    errores = 0
    archivos_pdf = []
    archivos_html = []

    if max_docs:
        ids_finales = ids_finales[:max_docs]

    total = len(ids_finales)
    for idx, pron_id in enumerate(ids_finales, start=1):
        try:
            detalle = obtener_detalle(pron_id)
            fecha = normalize(detalle.get("fecha"))[:10]
            if re.match(r"^\d{4}-\d{2}-\d{2}$", fecha):
                anio = int(fecha[:4])
                if not (desde <= anio <= hasta):
                    continue

            doc_id, ya_existia, pdf_path, html_path = guardar_pronunciamiento(detalle)
            codigo = normalize(detalle.get("codigoPronunciamiento")) or str(pron_id)
            fecha_log = fecha or "sin-fecha"
            archivos_pdf.append(pdf_path)
            archivos_html.append(html_path)

            if ya_existia:
                existentes += 1
                print(f"  [sync] [{idx}/{total}] {codigo} {fecha_log} -> {pdf_path}")
                log_scraper(
                    "judicial",
                    fecha_log[:4] if fecha_log[:4].isdigit() else 0,
                    codigo,
                    "sync",
                    DETALLE_WEB.format(id=pron_id),
                )
            else:
                nuevos += 1
                print(f"  [ok] [{idx}/{total}] {codigo} {fecha_log} -> {pdf_path}")
                log_scraper(
                    "judicial",
                    fecha_log[:4] if fecha_log[:4].isdigit() else 0,
                    codigo,
                    "ok",
                    DETALLE_WEB.format(id=pron_id),
                )
        except Exception as exc:
            errores += 1
            print(f"  [err] [{idx}/{total}] id={pron_id}: {exc}")
            try:
                log_scraper("judicial", 0, str(pron_id), "error", DETALLE_WEB.format(id=pron_id))
            except Exception:
                pass

        if delay > 0:
            time.sleep(delay)

    reporte = {
        "desde": desde,
        "hasta": hasta,
        "modo": info_ids["modo"],
        "ids_globales": len(ids_globales),
        "ids_arbol": len(ids_arbol),
        "ids_finales": len(ids_finales),
        "ids_extra_arbol": len(faltantes),
        "ids_faltantes_desde_buscador": faltantes,
        "nuevos": nuevos,
        "existentes": existentes,
        "errores": errores,
        "pdfs_generados": archivos_pdf,
        "html_generados": archivos_html,
        "generado_en": datetime.now().isoformat(),
    }
    reporte_path = guardar_reporte(reporte)

    print("[4/4] Resultado")
    print(f"      Nuevos: {nuevos}")
    print(f"      Ya existian: {existentes}")
    print(f"      Errores: {errores}")
    print(f"      Reporte: {reporte_path}")
    print("=" * 72)

    return reporte


def main():
    parser = argparse.ArgumentParser(description="Ingestar Jurisprudencia Judicial SII")
    parser.add_argument("--desde", type=int, default=1900, help="Anio inicial")
    parser.add_argument("--hasta", type=int, default=date.today().year, help="Anio final")
    parser.add_argument("--delay", type=float, default=0.0, help="Pausa entre llamadas")
    parser.add_argument("--max", type=int, default=None, help="Limitar cantidad de detalles")
    parser.add_argument("--ids", type=str, default="", help="IDs SII separados por coma")
    parser.add_argument(
        "--sin-verificacion",
        action="store_true",
        help="No recorrer el arbol normativa/articulo para validar completitud",
    )
    args = parser.parse_args()

    descargar_jurisprudencia_judicial(
        desde=args.desde,
        hasta=args.hasta,
        verificar_arbol=not args.sin_verificacion,
        delay=args.delay,
        max_docs=args.max,
        ids_explicitos=parse_ids(args.ids),
    )


if __name__ == "__main__":
    main()

