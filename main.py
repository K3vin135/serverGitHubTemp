from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import time
from os import access
import iot_api_client as iot
from iot_api_client.rest import ApiException
from iot_api_client.configuration import Configuration
import base64
from google.cloud import bigquery
import datetime

import functions_framework


# Ruta completa de BigQuery: proyecto.dataset.tabla
PROJECT_ID = "florexpotemp"
DATASET = "iot_dataset"
TABLE = "iot_dataset_table"


def generate_data():
    # Get your token 
    THING_ID="fa35cc83-f8c3-4b4d-8e33-432db36644bf"
    oauth_client = BackendApplicationClient(client_id="zCvVWlf186fmVSYWEL32F82vlhslElQ5")
    token_url = "https://api2.arduino.cc/iot/v1/clients/token"

    oauth = OAuth2Session(client=oauth_client)
    token = oauth.fetch_token(
        token_url=token_url,
        client_id="zCvVWlf186fmVSYWEL32F82vlhslElQ5",
        client_secret="h6pLTSjq1UEfjIzbmXKITv9eIF5jH10qiXPUADOHy0doAy7qLvTg6V3y3rvkAePs",
        include_client_id=True,
        audience="https://api2.arduino.cc/iot",
    )

    # store access token in access_token variable
    access_token = token.get("access_token")
    # print access token
    client_config = iot.Configuration(host="https://api2.arduino.cc")
    client_config.access_token = access_token
    client = iot.ApiClient(client_config)

    print(client)

    # ——————————————————————————
    # 5. (Opcional) Listar tus Things para verificar el ID
    # ——————————————————————————
    print("\n📋 Things disponibles:")
    things_api = iot.ThingsV2Api(client)
    try:
        things = things_api.things_v2_list(show_properties=False)
        for t in things:
            print(f" • {t.id}  →  {t.name}")
    except ApiException as e:
        print(f"Error listando Things [{e.status}]: {e.body}")

    # ——————————————————————————
    # 6. Listar las variables de tu Thing
    # ——————————————————————————
    print(f"\n🔎 Variables en el Thing {THING_ID}:")
    props_api = iot.PropertiesV2Api(client)
    try:
        props = props_api.properties_v2_list(id=THING_ID, show_deleted=False)
        if not props:
            print("  (No hay variables definidas en este Thing)")
        else:
            for p in props:
                print(f" • {p.variable_name}: {p.last_value}")
    except ApiException as e:
        print(f"Error listando propiedades [{e.status}]:\n{e.body}")

    # 4. Inserción estática en BigQuery (value siempre en columna 'value')
    bq_client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    rows = []
    for p in props:
        rows.append({
            "thingId": THING_ID,
            "value": round(float(p.last_value), 3),
            "ts": now
        })

    errors = bq_client.insert_rows_json(table_ref, rows)
    if errors:
        print("❌ Errores al insertar en BigQuery:", errors)
    else:
        print(f"✅ Insertadas {len(rows)} filas en {table_ref}")


         
# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    print(base64.b64decode(cloud_event.data["message"]["data"]))
    generate_data()
