import os
from google.oauth2 import service_account
import boto3
import pandas as pd
from io import StringIO
import datetime
from google.cloud import bigquery
from flask import Flask
import time # Import for time.sleep

# Variables de entorno de Google Cloud
GOOGLE_CLOUD_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT")

# Variables de entorno de AWS
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION")

# S3 y Athena
S3_BUCKET = os.environ.get("S3_BUCKET")
ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE")
ATHENA_TABLE = os.environ.get("ATHENA_TABLE")  # Nombre de la tabla de Athena
BIGQUERY_TABLE = os.environ.get("BIGQUERY_TABLE")

# Query de BigQuery fijo y no configurable
BIGQUERY_QUERY = f"SELECT * FROM `{BIGQUERY_TABLE}`"

app = Flask(__name__)

def get_athena_query_status(query_execution_id: str):
    """
    Checks the status of an Athena query execution.
    """
    athena_client = boto3.client(
        'athena',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
    return response['QueryExecution']['Status']['State']

def wait_for_athena_query(query_execution_id: str, timeout_seconds: int = 300):
    """
    Waits for an Athena query to complete and returns its final status.
    Raises an exception if the query fails or times out.
    """
    start_time = time.time()
    while True:
        status = get_athena_query_status(query_execution_id)
        print(f"DEBUG ATHENA: Query {query_execution_id} current status: {status}")
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if status == 'FAILED':
                athena_client = boto3.client(
                    'athena',
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    region_name=AWS_REGION
                )
                response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown Athena error.')
                raise Exception(f"Athena query {query_execution_id} failed: {error_message}")
            return status
        if time.time() - start_time > timeout_seconds:
            raise Exception(f"Athena query {query_execution_id} timed out after {timeout_seconds} seconds.")
        time.sleep(5) # Wait for 5 seconds before checking again


def execute_bigquery_query():
    """
    Ejecuta el query predefinido en BigQuery y devuelve los resultados como un DataFrame de Pandas.
    """
    print("Iniciando conexión con BigQuery...")
    # It's generally better to use GOOGLE_APPLICATION_CREDENTIALS environment variable
    # or let the client library find credentials automatically in Cloud Run.
    # If 'asistentes-digitales.json' is required, ensure it's properly deployed with the Cloud Run service.
    credentials = service_account.Credentials.from_service_account_file(
        "asistentes-digitales.json",
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    print(f"Ejecutando query en BigQuery: {BIGQUERY_QUERY}")
    query_job = client.query(BIGQUERY_QUERY)
    # Espera a que el trabajo de BigQuery termine y obtiene los resultados en un DataFrame
    results = query_job.result().to_dataframe()
    print(f"Query de BigQuery completado. Se obtuvieron {len(results)} filas.")

    #Logica adicional de python


    return results


def upload_dataframe_to_s3(df: pd.DataFrame, s3_key: str):
    """
    Sube un DataFrame de Pandas a S3 en formato CSV.
    El archivo CSV no incluye el índice y tiene cabecera.
    s3_key es la ruta completa del objeto en S3 (ej. 'carpeta/mi_archivo.csv')
    """
    print(f"Preparando DataFrame para subir a S3 como '{s3_key}'...")

    # --- Impresiones de depuración ---
    print(f"DEBUG S3: AWS_ACCESS_KEY_ID (primeros 4 chars): {AWS_ACCESS_KEY_ID[:4] if AWS_ACCESS_KEY_ID else 'None/Empty'}")
    print(f"DEBUG S3: AWS_SECRET_ACCESS_KEY (primeros 4 chars): {AWS_SECRET_ACCESS_KEY[:4] if AWS_SECRET_ACCESS_KEY else 'None/Empty'}")
    print(f"DEBUG S3: AWS_REGION: {AWS_REGION if AWS_REGION else 'None/Empty'}")
    print(f"DEBUG S3: S3_BUCKET: {S3_BUCKET if S3_BUCKET else 'None/Empty'}")

    if df.empty:
        print("DEBUG S3: El DataFrame está vacío. No se subirá ningún archivo a S3.")
        return None # O considera un error si no es un escenario esperado

    csv_buffer = StringIO()
    try:
        df.to_csv(csv_buffer, index=False, header=True)
        print("DEBUG S3: DataFrame convertido a CSV en buffer exitosamente.")
    except Exception as e:
        print(f"ERROR S3: Falló la conversión del DataFrame a CSV: {e}")
        raise

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        print("DEBUG S3: Cliente S3 inicializado exitosamente.")
    except Exception as e:
        print(f"ERROR S3: Falló la inicialización del cliente S3. ¿Credenciales o región incorrectas/faltantes?: {e}")
        raise

    try:
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_buffer.getvalue())
        s3_path = f"s3://{S3_BUCKET}/{s3_key}"
        print(f"Archivo subido exitosamente a S3: {s3_path}")
        return s3_path
    except Exception as e:
        print(f"ERROR S3: Falló la subida del archivo a S3. ¿Bucket o permisos incorrectos?: {e}")
        raise


def start_athena_query_execution(query_string: str):
    """
    Inicia la ejecución de una consulta en Athena.
    """
    print("Iniciando ejecución de consulta en Athena...")
    athena_client = boto3.client(
        'athena',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    # La ubicación de salida para los resultados de la consulta de Athena
    athena_output_location = f's3://{S3_BUCKET}/athena_query_results/' # Puedes cambiar esto si quieres que los resultados de Athena también vayan a una subcarpeta específica

    response = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': athena_output_location}
    )
    query_execution_id = response['QueryExecutionId']
    print(f"Consulta de Athena iniciada con ID: {query_execution_id}")
    return query_execution_id


@app.route("/")
def main():
    """
    Función principal que se ejecuta cuando el Cloud Run recibe una solicitud.
    Orquesta la lectura de BigQuery, la subida a S3 y la inserción en Athena.
    """
    try:
        print("Iniciando proceso de transferencia de datos de BigQuery a Athena...")

        # 1. Leer datos de BigQuery
        df_results = execute_bigquery_query()

        if df_results.empty:
            print("No se encontraron registros en BigQuery. Proceso finalizado.")
            return "No se encontraron registros en BigQuery.", 200

        # 2. Subir los resultados a S3
        # Generamos una subcarpeta única basada en la fecha y hora actual
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        unique_folder = f"temp_athena_load/{timestamp}/" # Una subcarpeta única para esta ejecución
        actual_s3_key = f"{unique_folder}data.csv" # El nombre del archivo dentro de la subcarpeta

        # Subimos el archivo a la subcarpeta única
        s3_full_path = upload_dataframe_to_s3(df_results, actual_s3_key)

        if s3_full_path is None:
            return "No se subió ningún archivo a S3 porque el DataFrame estaba vacío.", 200

        print(f"Uploaded CSV to S3: {s3_full_path}")

        # 3. Construir y ejecutar la consulta de inserción en Athena
        # La LOCATION para la tabla externa de Athena debe ser el directorio que contiene SOLO el archivo deseado.
        s3_table_location = f"s3://{S3_BUCKET}/{unique_folder}" # Apunta a la subcarpeta única
        print(f"DEBUG ATHENA: S3 Location para la tabla externa de Athena: {s3_table_location}")

        # Crear la tabla externa temporal con los tipos correctos para los datos del CSV
        # Se asume que el CSV tiene 'transferenciaasesor' y 'isderivacion' como booleanos, y 'duracion' como entero.
        create_table_query = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS temp_csv_source_table (
            timestamp STRING,
            sessionid STRING,
            lineanegocio STRING,
            motivoinicial STRING,
            respuesta STRING,
            transacciondurantellamada STRING,
            nombretransaccion STRING,
            concluyeenvoice STRING,
            transferenciaasesor BOOLEAN,
            datollave STRING,
            canal STRING,
            tramiteseleccionado STRING,
            tramiteaccion STRING,
            intentprevio STRING,
            isfallback STRING,
            fallbackmessage STRING,
            isderivacion STRING,
            duracion STRING,
            tiempo_por_sesion STRING,
            horafinal STRING,
            prestamoend STRING,
            flujoterminado STRING,
            year STRING,
            month STRING
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
            'separatorChar' = ',',
            'quoteChar' = '"',
            'escapeChar' = '\\\\'
        )
        LOCATION '{s3_table_location}' -- APUNTA A LA SUBCARPETA ÚNICA
        TBLPROPERTIES ('skip.header.line.count'='1', 'external.table.purge'='TRUE');
        """
        print(f"\nConsulta de creación de tabla enviada a Athena:\n {create_table_query}\n ---------------------\n")
        create_table_execution_id = start_athena_query_execution(create_table_query)
        create_status = wait_for_athena_query(create_table_execution_id)
        print(f"Consulta de creación de tabla completada con estado: {create_status}")


        # Insertar datos en la tabla de destino desde la tabla temporal
        # Se realizan CASTs explícitos para convertir BOOLEAN e INT a VARCHAR,
        # ya que la tabla de destino 'voice_asesor_qa' tiene todos los campos como STRING.
        insert_query = f"""
        INSERT INTO {ATHENA_DATABASE}.{ATHENA_TABLE}
        SELECT
            timestamp, sessionid, lineanegocio, motivoinicial, respuesta,
            transacciondurantellamada, nombretransaccion, concluyeenvoice,
            CAST(transferenciaasesor AS VARCHAR) AS transferenciaasesor, -- CAST a VARCHAR
            datollave, canal, tramiteseleccionado,
            tramiteaccion, intentprevio, isfallback, fallbackmessage,
            CAST(isderivacion AS VARCHAR) AS isderivacion, -- CAST a VARCHAR
            '' AS duracion, -- CAST(duracion AS VARCHAR) 
            '' AS tiempo_por_sesion,  --CAST(tiempo_por_sesion AS VARCHAR)
            '' AS  horafinal,  -- CAST(horafinal AS VARCHAR) 
            1 __index_level_0__, 
            prestamoend, flujoterminado, year, month
        FROM temp_csv_source_table;
        """
        print(f"Consulta de inserción enviada a Athena:\n {insert_query}\n---------------")
        insert_execution_id = start_athena_query_execution(insert_query)
        insert_status = wait_for_athena_query(insert_execution_id)
        print(f"Consulta de inserción completada con estado: {insert_status}")


        # Eliminar la tabla temporal después de la inserción
        drop_query = """
        DROP TABLE temp_csv_source_table;
        """
        print(f"\n--- Consulta de eliminación de tabla ---\n{drop_query}\n------------------\n")
        drop_execution_id = start_athena_query_execution(drop_query)
        drop_status = wait_for_athena_query(drop_execution_id)
        if drop_status != 'SUCCEEDED':
            print(f"ADVERTENCIA: La eliminación de la tabla temporal de Athena falló con estado: {drop_status}. Puede que necesites eliminarla manualmente.")
        print(f"Consulta de eliminación de tabla completada con estado: {drop_status}")

        print("Proceso completado exitosamente.")
        return f"Proceso completado. IDs de ejecución de consulta de Athena: Crear={create_table_execution_id} ({create_status}), Insertar={insert_execution_id} ({insert_status}), Eliminar={drop_execution_id} ({drop_status}).", 200

    except Exception as e:
        # Manejo de errores general
        error_message = f"Ocurrió un error durante la ejecución: {e}"
        print(error_message)
        return error_message, 500
