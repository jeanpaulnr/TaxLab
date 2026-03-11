# TaxLab IA v3 - Arquitectura

## Stack actual que se mantiene
- Flask como app server.
- SQLite como store principal.
- FTS5 para busqueda textual.
- Templates server-side con CSS propio.
- Scrapers existentes para SII y judicial.

## Shell de aplicacion
- /app: dashboard principal.
- /app/buscar: superficie principal de consulta documental.
- /app/documento/<id>: ficha canonica del documento.
- /app/asistente: chat tributario con evidencia.
- /app/casos: CRUD minimo de casos.
- /app/toolkit: herramientas tributarias.
- /admin/scraper: panel scraper existente.
- /admin/ingestion: stub para ingestion general.

## Capas
### Capa documental
- documentos
- docs_fts
- articulos_idx
- judicial_docs
- judicial_relaciones

### Capa de trabajo
- casos
- caso_notas

### Capa de enriquecimiento futuro
- organo_emisor
- criterio_principal
- sentido_criterio
- tema_central
- documento_relacionado

## RAG fase A
1. Consulta del usuario.
2. Busqueda FTS5 + filtros estructurados.
3. Seleccion de top documentos.
4. Extraccion de fragmentos relevantes.
5. Prompt de evidencia para Claude.
6. Respuesta estructurada con fuentes, riesgos y confianza.

## Restricciones
- No tocar engine.py.
- No tocar descargar_jurisprudencia_judicial.py.
- No mover a PostgreSQL en esta fase.
- Mantener todos los endpoints actuales operativos.

## Riesgo controlado
- Las rutas nuevas conviven con rutas legacy.
- Las migraciones son idempotentes.
- El backend actual de busqueda sigue siendo la fuente de verdad.
- La UI nueva se monta sobre templates Flask, no reemplaza el stack.

