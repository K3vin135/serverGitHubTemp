import base64
import pymssql
from datetime import datetime

def hello_pubsub(valor):


    # 2. Conecta a SQL Server
    conn = pymssql.connect(
        server   = 'fxlogdbsvr01.database.windows.net',  # tu servidor
        user     = 'AlejandroJ',                         # tu usuario
        password = 'Production2023*$',                   # tu contraseña
        database = 'KSDB01',
        port     = 1433
    )
    cursor = conn.cursor()

    try:
        # 3. Inserta el dato
        sql = """
        INSERT INTO staging.BuidPlanTEST
            ([TotalHours], [UserName], [TotalEmployeesProductionDate])
        VALUES
            (%s, 'Alejito', 20)
        """
        cursor.execute(sql,
            (valor)
        )

        # 4. Confirma
        conn.commit()
        print("Inserción realizada correctamente:", valor)

    except Exception as e:
        conn.rollback()
        print("Error al insertar:", e)

    finally:
        cursor.close()
        conn.close()


def hora_actual():
    from datetime import datetime
    import zoneinfo

    # Crea un tzinfo para Pacific Time (America/Los_Angeles)
    pacific = zoneinfo.ZoneInfo("America/Los_Angeles")

    # Obtiene la hora actual en PT y la formatea en ISO 8601
    now_pacific = datetime.now(pacific).isoformat()

    print(now_pacific)


hora_actual()