# crear archivo .env y escribir la informacion de su bd en caso de ser con usuario y contraseña
# SQL_SERVER_USER=
# SQL_SERVER_PASSWORD=
# SQL_SERVER_DB=
# SQL_SERVER_ENGINE={SQL Server}
# SQL_SERVER_SERVER=

#  --> si no está la carpeta espacio virtual  --> python -m venv espacio_virtual  
#escribir y ejectuar   .\espacio_virtual\Scripts\activate  --> sale asi despues (espacio_virtual) PS D:\VII SEMESTRE\proyecto_bd_TD>    
#pip install -r requirements.txt#py 
# .\descargar_y_normalizar.py 

import pandas as pd
import os
from datetime import datetime

# Crear carpeta de respaldo
fecha_hoy = datetime.now().strftime("%Y-%m-%d")
os.makedirs("respaldo", exist_ok=True)
os.makedirs("procesado", exist_ok=True)

#  Descargar el CSV directamente desde datos.gov.co
url_csv = "https://www.datos.gov.co/api/views/4v6r-wu98/rows.csv?accessType=DOWNLOAD"
df = pd.read_csv(url_csv)

# Guardar respaldo original
df.to_csv(f"respaldo/delitos_informaticos_{fecha_hoy}.csv", index=False)

# Asegurar que los nombres de columnas estén limpios (por si acaso)
df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')

#  Convertir 'cantidad' a numérico
df['cantidad'] = pd.to_numeric(df['cantidad'], errors='coerce')

#  Separar 'articulo' y 'descripcion_conducta'
df[['articulo', 'descripcion_conducta']] = df['descripcion_conducta'].str.extract(r'^(ARTICULO\s+\d+[A-Z]*)\.\s*(.*)$', expand=True)

#  Crear tablas normalizadas

# Crear tabla de departamentos
departamentos = df[['cod_depto', 'departamento']].drop_duplicates()

# Crear tabla de municipios
municipios = df[['cod_muni', 'municipio', 'cod_depto']].drop_duplicates()

# Crear tabla de conductas: usamos 'articulo' como ID
conductas = df[['articulo', 'descripcion_conducta']].drop_duplicates().reset_index(drop=True)

#tabla de hechos
hechos = df[['fecha_hecho', 'cod_muni', 'articulo', 'cantidad']].copy()
hechos.loc[:, 'id_hecho'] = hechos.index + 1

departamentos.to_csv("procesado/departamentos.csv", index=False)
municipios.to_csv("procesado/municipios.csv", index=False)
conductas.to_csv("procesado/conductas.csv", index=False)
hechos.to_csv("procesado/hechos.csv", index=False)

print("✅ Datos descargados, normalizados y exportados a CSV.")
