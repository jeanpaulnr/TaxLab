@echo off
chcp 65001 > nul
color 0A
echo.
echo  ╔══════════════════════════════════════════════════════╗
echo  ║       SII Normativa V2.0                            ║
echo  ║       Iniciando servidor...                          ║
echo  ╚══════════════════════════════════════════════════════╝
echo.
echo  URL: http://localhost:5000
echo  Scheduler diario activo: 08:00 AM
echo.
echo  Para detener: Ctrl+C
echo  ──────────────────────────────────────────────────────
echo.

cd /d "%~dp0"
python app.py

echo.
echo  Servidor detenido.
pause
