def connnector():
    try:
        import time
        import mysql.connector
        from mysql.connector import connect, Error 
        import config_real
        print('hello db_writerr')

        config = {
            'user': config_real.user,
            'password': config_real.password,
            'host': config_real.host,
            'port': config_real.port,
            'database': config_real.database,      
        }
        for _ in range(3):
            try:
                conn = mysql.connector.connect(**config)      
                print("Writerr connection established refactor")
                break
            except Error as e:
                print(f"Error connecting to MySQL: {e}")
                time.sleep(3)
                continue            
        try:
            cursor = conn.cursor() 
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
    except Exception as ex:
        print(f"30_writerr__{ex}")

    return conn, cursor

def db_wrtr(hotels_DB_refactor):
    from mysql.connector import connect, Error 
    try:
        conn, cursor = connnector()
    except:
        pass

    try:        
        update_table(conn, cursor, hotels_DB_refactor)
    except Exception as ex:
        print(f"45_writerr__{ex}")                 

    try:
        cursor.close()
        conn.close()
    except Error as e:
        print(f"Error connecting to MySQL: {e}")   

    return print("refactor upz_hotels was done successful!")

def update_table(conn, cursor, total):
    print("update_table start")
    try:
        update_query = "UPDATE upz_hotels SET fotos = %s, description = %s, facility = %s, otziv = %s, room = %s WHERE id = %s"
        batch_size = 500
        batch_values = []

        for i, item in enumerate(total):
            value = (item.get("fotos"), item.get("description"), item.get("facility"), item.get("otziv"), item.get("room"), item.get("id"))
            batch_values.append(value)

            if len(batch_values) >= batch_size:
                try:
                    for _ in range(5):
                        try:                        
                            cursor.executemany(update_query, batch_values)
                            conn.commit()
                            batch_values = []
                            print(f"Success batch__{i}")
                            break
                        except:
                            try:
                                conn, cursor = connnector()
                                continue
                            except:
                                pass

                except Exception as ex:
                    print(f"Error executing update query: {ex}")
                    insert_rows_individually(conn, cursor, update_query, batch_values)
                    batch_values = []

        if batch_values:
            try:
                for _ in range(5):
                    try:                        
                        cursor.executemany(update_query, batch_values)
                        conn.commit()
                        batch_values = []
                        print(f"Success batch__{len(total)}")
                        break
                    except:
                        try:
                            conn, cursor = connnector()
                            continue
                        except:
                            pass

            except Exception as ex:
                print(f"Error executing update query: {ex}")
                insert_rows_individually(conn, cursor, update_query, batch_values)      

        print("Update completed successfully.")

    except Exception as ex:
        print(f"Error executing update query: {ex}")

    return

def insert_rows_individually(conn, cursor, query, data):
    try:
        for item in data:
            try:
                cursor.execute(query, item)
            except Exception as ex:
                # print(f"204___: {ex}")
                continue
        conn.commit()

    except Exception as ex:
        print(ex)   

    return