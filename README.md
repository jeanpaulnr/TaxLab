# SII Normativa V2.0
## Sistema de Búsqueda Jurídica Tributaria con Scraping Automático

---

## ¿Qué hace este sistema?

| Función | Detalle |
|---------|---------|
| **Scraping real** | Descarga PDFs directamente de sii.cl (circulares, oficios, resoluciones) |
| **100% exactitud** | Extracción con PyMuPDF desde PDF digital nativo — sin OCR, sin pérdida |
| **Actualización diaria** | Scheduler verifica novedades a las 08:00 AM automáticamente |
| **Búsqueda FTS5** | Full-text search con SQLite FTS5 sobre todo el texto |
| **Cruce de normas** | Relaciona documentos que citan los mismos artículos |
| **Citas formales** | Genera 4 formatos de cita para responder al SII |
| **Historial de normas** | Muestra evolución de circulares/oficios del mismo número |

---

## Instalación (Windows)

```
1. Doble clic en INSTALAR.bat
2. Esperar que termine
3. Doble clic en INICIAR.bat
4. Abrir http://localhost:5000
```

O manualmente:
```bash
pip install flask requests beautifulsoup4 lxml PyMuPDF schedule
python app.py
```

---

## Estrategia de descarga recomendada

El SII no tiene una API pública para jurisprudencia. Las URLs de los PDFs siguen un patrón predecible:

```
Circulares:    sii.cl/normativa_legislacion/circulares/2024/circu31.pdf
Oficios LIR:   sii.cl/normativa_legislacion/jurisprudencia_administrativa/lir/2024/ja1234.pdf
Resoluciones:  sii.cl/normativa_legislacion/resoluciones/2024/reso45.pdf
```

### Paso 1 — Verificación inicial (5-10 minutos)
```
Panel Scraper → "Descarga por año" → Circulares → 2024 → Iniciar
```
Verifica que los PDFs se descargan y el texto aparece en búsqueda.

### Paso 2 — Años recientes (1-2 horas)
```
Panel Scraper → Descarga histórica → 2020-2025 → Todos los tipos → Iniciar
```
~5 años completos. Suficiente para trabajo diario.

### Paso 3 — Histórico completo (correr de noche, 24-48 hrs)
```
Descarga histórica → 1990-2019 → delay 1.5s → Iniciar
```
Deja corriendo. El sistema sigue aunque cierres el navegador.

### Paso 4 — Mantenimiento automático
El scheduler verifica novedades cada día a las 08:00 AM.
Solo descarga lo que no existe aún (hash MD5 deduplicación).

---

## Volumen estimado

| Tipo | Años | Docs/año | Total estimado |
|------|------|----------|----------------|
| Circulares | 35 años | ~55 | ~1.900 |
| Resoluciones | 35 años | ~150 | ~5.250 |
| Oficios LIR | 35 años | ~800 | ~28.000 |
| Oficios IVA | 35 años | ~400 | ~14.000 |
| Oficios CT | 35 años | ~300 | ~10.500 |
| **Total** | | | **~60.000** |

Espacio en disco estimado: ~8-15 GB (PDFs + SQLite)

---

## Búsqueda — ejemplos útiles

| Búsqueda | Resultado esperado |
|----------|--------------------|
| `artículo 31 gastos necesarios` | Todas las circulares y oficios sobre art. 31 LIR |
| `Pro Pyme 14 D requisitos` | Normativa del régimen 14 D |
| `prescripción 6 años maliciosamente` | Art. 200 CT plazo extraordinario |
| `IVA servicios digitales plataformas` | Art. 8° letra n) LIVS |
| `ganancias de capital acciones 2022` | Art. 107 LIR reforma |

---

## Troubleshooting

**El scraper devuelve HTTP 404:**
Los oficios no tienen numeración consecutiva. Los gaps (ej. 1001 al 1050 no existen) son normales. El scraper los omite automáticamente.

**El scraper devuelve HTTP 403:**
sii.cl detectó demasiados requests. Aumenta el delay a 2-3 segundos y espera 30 minutos antes de reintentar.

**El texto extraído tiene caracteres raros:**
Algunos PDFs antiguos (pre-2000) pueden estar escaneados. En ese caso el texto estará vacío. Es normal para documentos históricos muy antiguos.

**La app no inicia:**
```bash
pip install -r requirements.txt
python app.py
```

---

## Estructura del proyecto

```
sii_normativa_v2/
├── app.py              ← Flask + scheduler diario
├── requirements.txt
├── INSTALAR.bat
├── INICIAR.bat
├── data/
│   └── sii_normativa.db   ← SQLite con FTS5
├── pdfs/              ← PDFs descargados
├── logs/              ← Log diario del scraper
├── scraper/
│   └── engine.py      ← Motor de scraping + PyMuPDF
└── templates/
    ├── index.html
    ├── documento.html
    ├── scraper.html
    └── agregar.html
```

---

## Stack tecnológico

| Componente | Tecnología | Por qué |
|-----------|-----------|---------|
| Web app | Flask (Python) | Simple, liviano, sin dependencias pesadas |
| Base de datos | SQLite + FTS5 | Sin servidor, búsqueda full-text nativa |
| Extracción PDF | PyMuPDF | 100% fidelidad en PDFs digitales nativos |
| Scraping | requests + BeautifulSoup | Estable, sin Selenium ni JavaScript |
| Scheduler | schedule | Tarea diaria sin cron ni servicios externos |

---

*SII Normativa V2.0 — Herramienta para uso profesional del contador*
