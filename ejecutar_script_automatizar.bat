@echo off
REM Activar entorno virtual
call "D:\\VII SEMESTRE\\proyecto_bd_TD\\espacio_virtual\\Scripts\\activate"

REM Ir a la carpeta del proyecto
cd /d "D:\\VII SEMESTRE\\proyecto_bd_TD"

REM Ejecutar el script
python .\\descargar_y_normalizar.py 

REM Ejecutar el script
python .\\enviar_sqlServer.py

REM Desactivar entorno 
deactivate