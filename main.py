import db_reader, db_writerrr
import sys
import time
import math
import numpy as np
# import concurrent.futures

def refactor_url_hotels(hotels_DB, foto_list, descr_list, faci_list, rew_list, room_total_list):   
    start_time = time.time()
    # Преобразуем списки в массивы NumPy
    foto_array = np.array(foto_list)
    descr_array = np.array(descr_list)
    faci_array = np.array(faci_list)
    rew_array = np.array(rew_list)
    room_array = np.array(room_total_list)

    # Создаем словари с флагами присутствия элементов
    foto_dict = {item_id: True for item_id in foto_list}
    descr_dict = {item_id: True for item_id in descr_list}
    faci_dict = {item_id: True for item_id in faci_list}
    rew_dict = {item_id: True for item_id in rew_list}
    room_dict = {item_id: True for item_id in room_total_list}

    for item in hotels_DB:
        
        item_id = item["hotel_id"]

        # Проверяем наличие item_id в соответствующем массиве
        flag_foto = np.any(foto_array == item_id)
        flag_descr = np.any(descr_array == item_id)
        flag_faci = np.any(faci_array == item_id)
        flag_rew = np.any(rew_array == item_id)
        flag_room = np.any(room_array == item_id)

        # Если элемент не найден в массиве, проверяем наличие в словаре
        if not flag_foto:
            flag_foto = item_id in foto_dict
        if not flag_descr:
            flag_descr = item_id in descr_dict
        if not flag_faci:
            flag_faci = item_id in faci_dict
        if not flag_rew:
            flag_rew = item_id in rew_dict
        if not flag_room:
            flag_room = item_id in room_dict

        try:
            # Преобразуем в целое число (1 или 0)
            item["fotos"] = int(flag_foto)  
            item["description"] = int(flag_descr)
            item["facility"] = int(flag_faci)
            item["otziv"] = int(flag_rew)
            item["room"] = int(flag_room)
            item["refactor"] = True
        except Exception as ex:
            print(ex)

    finish_time = time.time() - start_time
    print(f"Refactor time: {math.ceil(finish_time)} сек")
    hotels_DB_sorted = sorted(hotels_DB, key=lambda x: x["id"])

    return hotels_DB_sorted


def main():
    hotels_DB, foto_list, descr_list, faci_list, rew_list, room_total_list = db_reader.db_opener()
    hotels_DB_refactor = refactor_url_hotels(hotels_DB, foto_list, descr_list, faci_list, rew_list, room_total_list)
    db_writerrr.db_wrtr(hotels_DB_refactor)

if __name__ == "__main__":
    start_time = time.time() 
    main() 
    finish_time = time.time() - start_time
    print(f"Total time:  {math.ceil(finish_time)} сек")
    sys.exit()
   




        