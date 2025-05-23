import logging
import os
import datetime

import azure.functions as func
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import iot_api_client as iot
from iot_api_client.rest import ApiException
import pyodbc

# Configuración de Arduino IoT
THING_ID = "fa35cc83-f8c3-4b4d-8e33-432db36644bf"
CLIENT_ID = "zCvVWlf186fmVSYWEL32F82vlhslElQ5"
CLIENT_SECRET = "h6pLTSjq1UEfjIzbmXKITv9eIF5jH10qiXPUADOHy0doAy7qLvTg6V3y3rvkAePs"

# Nombre de la variable de entorno que contiene tu connection string
ENV_DB_CONN = "DB_CONN_ENV"

# Obtener la conexión cuando se carga el módulo
conn_str = os.getenv(ENV_DB_CONN)
if not conn_str:
    raise RuntimeError(f"La variable de entorno {ENV_DB_CONN} no está definida.")

app = func.FunctionApp()

@app.timer_trigger(
    schedule="0 */1 * * * *",  # cada minuto
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    generate_data()
    logging.info('Python timer trigger function executed.')


def generate_data():
    # --- 1) Autenticación con Arduino IoT Cloud ---
    oauth_client = BackendApplicationClient(client_id=CLIENT_ID)
    oauth = OAuth2Session(client=oauth_client)
    token = oauth.fetch_token(
        token_url="https://api2.arduino.cc/iot/v1/clients/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        include_client_id=True,
        audience="https://api2.arduino.cc/iot"
    )
    client_config = iot.Configuration(host="https://api2.arduino.cc")
    client_config.access_token = token.get("access_token")
    client = iot.ApiClient(client_config)

    # --- 2) Listar propiedades del Thing ---
    props_api = iot.PropertiesV2Api(client)
    try:
        props = props_api.properties_v2_list(id=THING_ID, show_deleted=False)
    except ApiException as e:
        logging.error(f"Error listando propiedades [{e.status}]: {e.body}")
        return

    # --- 3) Preparar datos a insertar ---
    pacific = datetime.timezone(datetime.timedelta(hours=-7))
    now = datetime.datetime.now(pacific)

    rows = []
    for p in props:
        rows.append({
            "thingId": THING_ID,
            "value": round(float(p.last_value), 3) + 20,
            "ts": now
        })

    # --- 4) Insertar en SQL Server ---
    insert_into_sql(rows)


def insert_into_sql(rows):
    # Usar conn_str cargada al inicio
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        for row in rows:
            cursor.execute(
                "INSERT INTO [dbo].[TempData] ([thingId],[value],[ts]) VALUES (?,?,?)",
                row["thingId"], row["value"], str(row["ts"])
            )
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Inserted {len(rows)} rows into SQL Server")
    except Exception as e:
        logging.error(f"Error inserting into SQL Server: {e}")
