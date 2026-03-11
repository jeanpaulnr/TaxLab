# TaxLab IA v3 - Backlog inicial

## PR 1 - Navegacion y shell
- [x] Crear base.html.
- [x] Crear /app, /app/buscar, /admin/scraper, /admin/ingestion.
- [x] Redirigir /, /documento/<id> y /scraper a rutas canonicas.
- [x] Adaptar index.html, documento.html y scraper.html para extender base.

## PR 2 - Asistente con evidencia
- [x] Crear rag.py.
- [x] Crear /api/chat.
- [x] Crear asistente.html.
- [x] Manejar ausencia de ANTHROPIC_API_KEY sin romper la UI.

## PR 3 - Casos
- [x] Crear migraciones para casos y columnas nuevas.
- [x] CRUD minimo en /app/casos.
- [x] Vista detalle simple por caso.
- [x] Asociar documento a caso como nota/documento.

## PR 4 - Toolkit e ingestion
- [x] Crear toolkit.html con stubs.
- [x] Crear admin_ingestion.html.
- [x] Dejar TODOs claros para calculadoras y calendario.

## Validacion manual pendiente
- [ ] python app.py inicia sin errores.
- [ ] /app carga.
- [ ] /app/buscar carga y la busqueda sigue usando /buscar.
- [ ] /app/documento/<id> funciona.
- [ ] /admin/scraper funciona.
- [ ] /app/asistente carga con y sin API key.
- [ ] /app/casos crea casos.
- [ ] migraciones.py corre sin error multiples veces.
- [ ] diagnostico.py mantiene el mismo numero de documentos.
