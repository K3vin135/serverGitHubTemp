import logging
import azure.functions as func
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import time
import os, json
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
app = func.FunctionApp()

@app.timer_trigger(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=True) 
def timer_triggerAZ(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    generate_data()
    logging.info('Python timer trigger function executed.')


def generate_data():
    get_bq_client()
    # Get your token
    THING_ID = "fa35cc83-f8c3-4b4d-8e33-432db36644bf"
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
    # 5. (Optional) List your Things to verify the ID
    # ——————————————————————————
    print("\n Available Things:")
    things_api = iot.ThingsV2Api(client)
    try:
        things = things_api.things_v2_list(show_properties=False)
        for t in things:
            print(f"  {t.id}  ,  {t.name}")
    except ApiException as e:
        print(f"Error listing Things [{e.status}]: {e.body}")

    # ——————————————————————————
    # 6. List the variables of your Thing
    # ——————————————————————————
    print(f"\n Variables in Thing {THING_ID}:")
    props_api = iot.PropertiesV2Api(client)
    try:
        props = props_api.properties_v2_list(id=THING_ID, show_deleted=False)
        if not props:
            print("  (No variables defined in this Thing)")
        else:
            for p in props:
                print(f" • {p.variable_name}: {p.last_value}")
    except ApiException as e:
        print(f"Error listing properties [{e.status}]:\n{e.body}")

    # 4. Static insertion into BigQuery (value always in column 'value')
    bq_client = get_bq_client()
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    
    pacific = datetime.timezone(datetime.timedelta(hours=-7))
    now = datetime.datetime.now(pacific).isoformat()

    rows = []
    for p in props:
        rows.append({
            "thingId": THING_ID,
            "value": round(float(p.last_value), 3)+20,
            "ts": now
        })

    errors = bq_client.insert_rows_json(table_ref, rows)
    if errors:
        print(" Errors inserting into BigQuery:", errors)
    else:
        print(f" Inserted {len(rows)} rows into {table_ref}")

def get_bq_client():
    # 1) Leer la variable de entorno que definiste en Azure
    key_json = os.environ["BIGQUERY_KEY"]
    # 2) Parsear el JSON en un dict de Python
    info = json.loads(key_json)
    # 3) Construir el cliente usando las credenciales en memoria
    return bigquery.Client.from_service_account_info(info)