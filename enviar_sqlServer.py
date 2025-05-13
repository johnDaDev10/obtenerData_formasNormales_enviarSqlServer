# Importamos la librería para conectar con SQL Server
# pip install pyodbc

from dotenv import load_dotenv
import pandas as pd
import os 
import pyodbc

# os para poder guardar las credenciales como variables de entorno en Windows y luego acceder a ellas.
# pip install python-dotenv

# Cargar variables desde el archivo .env
load_dotenv()

# Obtener credenciales desde las variables de entorno
usuario = os.environ.get("SQL_SERVER_USER")
contraseña = os.environ.get("SQL_SERVER_PASSWORD")
db = os.environ.get("SQL_SERVER_DB")
engine = os.environ.get("SQL_SERVER_ENGINE")
server = os.environ.get("SQL_SERVER_SERVER")

# Conectar con SQL Server,si es Windows auth --> Trusted_Connection=yes
try:
    # conexion = pyodbc.connect("DRIVER={SQL Server};SERVER={server};DATABASE={db};Trusted_Connection=yes")
    conexion = pyodbc.connect(
    f"DRIVER={engine};"
    f"SERVER={server};"
    f"DATABASE={db};"
    f"UID={usuario};"
    f"PWD={contraseña};")

    print("Conexión exitosa")
    cursor = conexion.cursor()

    # Crear tablas
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='departamentos' AND xtype='U')
    CREATE TABLE departamentos (
        cod_depto INT PRIMARY KEY,
        departamento NVARCHAR(70)
    )
    """)

    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='municipios' AND xtype='U')
    CREATE TABLE municipios (
        cod_muni INT PRIMARY KEY,
        municipio NVARCHAR(70),
        cod_depto INT FOREIGN KEY REFERENCES departamentos(cod_depto)
    )
    """)

    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='conductas' AND xtype='U')
    CREATE TABLE conductas (
        articulo NVARCHAR(20) PRIMARY KEY,
        descripcion_conducta NVARCHAR(600)
    )
    """)

    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='hechos' AND xtype='U')
    CREATE TABLE hechos (
        id_hecho INT PRIMARY KEY,
        fecha_hecho DATE,
        cod_muni INT FOREIGN KEY REFERENCES municipios(cod_muni),
        articulo NVARCHAR(20) FOREIGN KEY REFERENCES conductas(articulo),
        cantidad INT
    )
    """)
    
    conexion.commit()

    # reiniciar todas las tablas
    for table, csv_file in {
    "hechos": "hechos.csv",
    "municipios": "municipios.csv",
    "departamentos": "departamentos.csv",
    "conductas": "conductas.csv",
    }.items():
        print(f"reiniciando datos de la tabla {table}...")
        cursor.execute(f"DELETE FROM {table}")
        
    conexion.commit()

    # Cargar y enviar CSVs
    data_path = "procesado"

    for table, csv_file in {
        "departamentos": "departamentos.csv",
        "municipios": "municipios.csv",
        "conductas": "conductas.csv",
        "hechos": "hechos.csv"
    }.items():
        df = pd.read_csv(f"{data_path}/{csv_file}")
        print(f"Ingresando datos en {table}...")

        for index, row in df.iterrows():
            placeholders = ", ".join(["?"] * len(row))
            cols = ", ".join(row.index)
            sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
            cursor.execute(sql, *row)

        conexion.commit()

    cursor.close()
except Exception as ex:
    print(ex)
finally:
    conexion.close()
    print("Conexion Finalizada.")




#procesado

"""    for table, csv_file in {
        "departamentos": "departamentos.csv",
        "municipios": "municipios.csv",
        "conductas": "conductas.csv",
        "hechos": "hechos.csv"
    }.items():
        df = pd.read_csv(f"{data_path}/{csv_file}")
        print(f"Ingresando datos en {table}...")

        for index, row in df.iterrows():
            placeholders = ", ".join(["?"] * len(row))
            cols = ", ".join(row.index)
            sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
            cursor.execute(sql, *row)

        conexion.commit()

    cursor.close()"""
    

    #for table, csv_file in {
    # "departamentos": "departamentos.csv",
    # "municipios": "municipios.csv",
    # "conductas": "conductas.csv",
    # "hechos": "hechos.csv"
    # }.items():
    #     df = pd.read_csv(f"{data_path}/{csv_file}")
    #     print(f"Ingresando datos en {table}...")

    #     # Eliminar columna ID si es identidad (para hechos)
    #     if table == "hechos":
    #         df = df.drop(columns=["id"], errors="ignore")

    #     for _, row in df.iterrows():
    #         if table == "departamentos":
    #             cursor.execute("""
    #                 IF NOT EXISTS (
    #                     SELECT 1 FROM departamentos WHERE cod_depto = ?
    #                 )
    #                 INSERT INTO departamentos (cod_depto, departamento) VALUES (?, ?)
    #             """, row["cod_depto"], row["cod_depto"], row["departamento"])

    #         elif table == "municipios":
    #             cursor.execute("""
    #                 IF NOT EXISTS (
    #                     SELECT 1 FROM municipios WHERE cod_muni = ?
    #                 )
    #                 INSERT INTO municipios (cod_muni, municipio, cod_depto) VALUES (?, ?, ?)
    #             """, row["cod_muni"], row["cod_muni"], row["municipio"], row["cod_depto"])

    #         elif table == "conductas":
    #             cursor.execute("""
    #                 IF NOT EXISTS (
    #                     SELECT 1 FROM conductas WHERE articulo = ?
    #                 )
    #                 INSERT INTO conductas (articulo, descripcion_conducta) VALUES (?, ?)
    #             """, row["articulo"], row["articulo"], row["descripcion_conducta"])

    #         elif table == "hechos":
    #             cursor.execute("""
    #                 IF NOT EXISTS (
    #                     SELECT 1 FROM hechos
    #                     WHERE fecha_hecho = ? AND cod_muni = ? AND articulo = ?
    #                 )
    #                 INSERT INTO hechos (fecha_hecho, cod_muni, articulo, cantidad) VALUES (?, ?, ?, ?)
    #             """, row["fecha_hecho"], row["cod_muni"], row["articulo"],
    #                 row["fecha_hecho"], row["cod_muni"], row["articulo"], row["cantidad"])

    #     conexion.commit()

    # cursor.close()