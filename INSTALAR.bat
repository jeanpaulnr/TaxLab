@echo off
chcp 65001 > nul
color 0A
echo.
echo  ╔══════════════════════════════════════════════════════╗
echo  ║       SII Normativa V2.0 — Instalador               ║
echo  ║       Sistema de Búsqueda Jurídica Tributaria        ║
echo  ╚══════════════════════════════════════════════════════╝
echo.

REM Verificar Python
python --version > nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python no encontrado. Instala Python 3.10+ desde python.org
    pause
    exit /b 1
)

echo  [1/4] Python detectado OK
echo.

REM Crear directorios
if not exist "data" mkdir data
if not exist "pdfs" mkdir pdfs
if not exist "logs" mkdir logs
if not exist "scraper" mkdir scraper

echo  [2/4] Directorios creados
echo.

REM Instalar dependencias
echo  [3/4] Instalando dependencias (puede tomar 2-3 minutos)...
pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo  [ERROR] Fallo al instalar dependencias.
    echo  Intenta manualmente: pip install flask requests beautifulsoup4 lxml PyMuPDF schedule
    pause
    exit /b 1
)

echo  [4/4] Dependencias instaladas OK
echo.
echo  ══════════════════════════════════════════════════════
echo  ✓ Instalación completada
echo  
echo  Para iniciar: ejecuta INICIAR.bat
echo  Luego abre: http://localhost:5000
echo  ══════════════════════════════════════════════════════
echo.
pause
