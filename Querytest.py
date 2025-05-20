import pyodbc

def SQL_insert(data):
    query = """
INSERT INTO staging.BuidPlanTEST ([TotalHours],[UserName],[TotalEmployeesProductionDate]) VALUES (?,'Alejito',20)
 
 """
    
    # 2. Cadena de conexión
    connection_string = (
        "Driver={SQL Server};"
        "Server=fxlogdbsvr01.database.windows.net;"
        "Database=KSDB01;"
        "UID=AlejandroJ;"
        "PWD=Production2023*$;"
    )

    # 3. Abre la conexión   
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    print(data)
    try:


        # 5. Ejecuta el INSERT
        cursor.execute(query,
                       data)
                    

        # 6. Confirma (commit) los cambios
        conn.commit()
        print("Inserción realizada correctamente.")

    except Exception as e:
        # En caso de error, revertir y mostrar
        conn.rollback()
        print("Error al insertar:", e)

    finally:
        # 7. Cierra cursor y conexión
        cursor.close()
        conn.close()

SQL_insert(77)