from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import time
from os import access
import iot_api_client as iot
from iot_api_client.rest import ApiException
from iot_api_client.configuration import Configuration



while True:
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
    props_api = iot.PropertiesV2Api(client)
    try:
        things = things_api.things_v2_list(show_properties=False)
        for t in things:
            props = props_api.properties_v2_list(id=t.id, show_deleted=False)
            for p in props:
                print(f" • {t.id}  →  {t.name} ({p.variable_name} = {p.last_value})")
    except ApiException as e:
        print(f"Error listando Things [{e.status}]: {e.body}")

    # ——————————————————————————
    # 6. Listar las variables de tu Thing
    # # ——————————————————————————
    # print(f"\n🔎 Variables en el Thing {THING_ID}:")
    # props_api = iot.PropertiesV2Api(client)
    # try:
    #     props = props_api.properties_v2_list(id=THING_ID, show_deleted=False)
    #     if not props:
    #         print("  (No hay variables definidas en este Thing)")
    #     else:
    #         for p in props:
    #             print(f" • {p.variable_name}: {p.last_value}")
    # except ApiException as e:
    #     print(f"Error listando propiedades [{e.status}]:\n{e.body}")
    time.sleep(10)  