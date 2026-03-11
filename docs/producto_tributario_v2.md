# TaxLab IA v3 - Producto

## Proposito
Evolucionar SII Normativa desde buscador documental a plataforma de inteligencia tributaria chilena con foco en evidencia, trazabilidad y flujos reales de trabajo para contadores y abogados tributaristas.

## Pilar de producto
- Base publica verificable: corpus SII + jurisprudencia judicial con fuente y descarga.
- Asistente con evidencia: respuestas solo con documentos del corpus y cita obligatoria.
- Casos: espacios privados por cliente/caso para notas, documentos y consultas guardadas.
- Toolkit: utilidades tributarias de alto uso y bajo tiempo de aprendizaje.
- Operacion confiable: scraping e ingestion visibles, auditables y no invasivos.

## Sprint 1
Objetivo: separar navegacion, preparar RAG y dejar la primera capa de Casos/Toolkit sin romper scraping ni busqueda.

### Entregables
- Shell de navegacion TaxLab IA.
- Dashboard /app.
- Busqueda canonica en /app/buscar con backend actual.
- Detalle canonico en /app/documento/<id>.
- Asistente tributario como stub funcional con endpoint /api/chat.
- Casos con CRUD minimo.
- Toolkit con stubs de utilidades.
- Admin /admin/scraper y /admin/ingestion.
- Migraciones idempotentes para columnas nuevas y tablas de casos.

## Filosofia del asistente
- No inventar.
- Citar siempre.
- Diferenciar norma, criterio administrativo, jurisprudencia e interpretacion.
- Declarar incertidumbre cuando falte evidencia o exista contradiccion.

## Fuera de alcance por ahora
- Auth multiusuario.
- Billing y cuotas.
- PostgreSQL/pgvector.
- Enriquecimiento masivo por IA del corpus.
- Upload de archivos privados a Casos.

