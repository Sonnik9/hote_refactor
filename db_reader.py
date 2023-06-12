def db_opener():
    hotels_DB = []
    foto_set = set()
    foto_list = []
    descr_set = set()
    descr_list = []
    faci_set = set()
    faci_list = []
    rew_set = set()
    rew_list = []
    rooms_set = set()
    rooms_list = []
    blocks_set = set()
    blocks_list = []
    room_total_list = []
    try:
        import time
        import mysql.connector
        from mysql.connector import connect, Error 
        import config_real
          
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
                print("REader connection established")
                break
            except Error as e:
                print(f"Error connecting to MySQL: {e}")
                time.sleep(3)
                continue           
        
        try:
            cursor = conn.cursor() 
        except Error as e:
            print(f"Error connecting to MySQL: {e}")

        try:
            select_query1  = "SELECT id, hotel_id, fotos, description, facility, otziv, room FROM upz_hotels "
            cursor.execute(select_query1)
            hotels_data = cursor.fetchall()
        except Exception as e:
            print(f"db_reader___str46: {e}")

        try:
            for item in hotels_data:
                hotels_DB.append({
                    'id': item[0],
                    'hotel_id': item[1],                  
                    # 'fotos': item[2],
                    # 'description': item[3],
                    # 'facility': item[4],
                    # "otziv": item[5],
                    # 'room': item[6]                    
                })
               
        except Exception as ex:
            print(f"source_DB_hotel_68str___{ex}") 

        print(f'{len(hotels_DB)}')

        try:
            select_query2  = "SELECT hotelid FROM upz_hotels_photos "
            cursor.execute(select_query2)
            fotos_data = cursor.fetchall()
        except Exception as e:
            print(f"db_reader___str77: {e}")

        try:
            for hotel_id in fotos_data:
                foto_set.add(hotel_id[0])             
        except Exception as ex:
            print(f"source_DB_hotel_87str___{ex}") 

        foto_list = list(foto_set)
        print(f"foto_len___{len(foto_list)}")

        try:
            select_query3  = "SELECT hotelid FROM upz_hotels_description "
            cursor.execute(select_query3)
            descr_data = cursor.fetchall()
        except Exception as e:
            print(f"db_reader___str97: {e}")

        try:
            for hotel_id in descr_data:
                descr_set.add(hotel_id[0])              
        except Exception as ex:
            print(f"source_DB_hotel_107str___{ex}") 

        descr_list = list(descr_set)
        print(f"descr_len__{len(descr_list)}")
        try:
            select_query4  = "SELECT hotelid FROM upz_hotels_facilityties "
            cursor.execute(select_query4)
            faci_data = cursor.fetchall()
        except Exception as e:
            print(f"db_reader___str116: {e}")

        try:
            for hotel_id in faci_data:
                faci_set.add(hotel_id[0])              
        except Exception as ex:
            print(f"source_DB_hotel_126str___{ex}") 

        faci_list = list(faci_set)
        print(f"faci_len___{len(faci_list)}")
        try:
            select_query5  = "SELECT hotelid FROM upz_hotels_review "
            cursor.execute(select_query5)
            rew_data = cursor.fetchall()
        except Exception as e:
            print(f"db_reader___str4135: {e}")

        try:
            for hotel_id in rew_data:
                rew_set.add(hotel_id[0])             
        except Exception as ex:
            print(f"source_DB_hotel_145str___{ex}") 

        rew_list = list(rew_set)
        print(f"rew_len___{len(rew_list)}")
        try:
            select_query6  = "SELECT hotelid FROM upz_hotels_rooms "
            cursor.execute(select_query6)
            rooms_data = cursor.fetchall()
        except Exception as e:
            print(f"db_reader___str154: {e}")

        try:
            for hotel_id in rooms_data: 
                rooms_set.add(hotel_id[0])           
        except Exception as ex:
            print(f"source_DB_hotel_164str___{ex}") 

        rooms_list = list(rooms_set)
        print(f"rooms_len___{len(rooms_list)}")

        try:
            select_query7  = "SELECT hotelid FROM upz_hotels_rooms_blocks "
            cursor.execute(select_query7)
            blocks_data = cursor.fetchall()
        except Exception as e:
            print(f"db_reader___str174: {e}")

        try:
            for hotel_id in blocks_data:
                blocks_set.add(hotel_id[0])              
        except Exception as ex:
            print(f"source_DB_hotel_184str___{ex}") 

        blocks_list = list(blocks_set)
        print(f"blocks_len___{len(blocks_list)}")

        try:
            room_total_set = rooms_set & blocks_set
            room_total_list = list(room_total_set)
            print(f"room_total_len___ {len(room_total_list)}")
        except:
            pass
        try:
            cursor.close()
            conn.close()    
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
        try:
            print(f"data_DB _____{hotels_DB[0]}")
            print(f"data_DB _____{hotels_DB[-1]}")
        except:
            pass
        return hotels_DB, foto_list, descr_list, faci_list, rew_list, room_total_list
    except:
        return None


# db_opener()
        

# python db_reader.py
# python -m db_all.db_reader
