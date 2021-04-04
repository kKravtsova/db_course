import psycopg2
from psycopg2 import Error
import pandas as pd
import re
import funcs

creatingTable = True # Генерация таблички
fillingTable = True # Заполнение данными из файлов
firstFile = "Odata2020File.csv"
secondFile = "Odata2019File.csv"
bd_table = "odata_zno" # Название создаваемой таблицы
writingBest = True # Вывод в консоль лучших по регионам

try:
    # Подключение к существующей базе данных
    connection = psycopg2.connect(user="postgres",
                                password="1111",
                                host="127.0.0.1",
                                port="5432",
                                database="lab_1")
    cursor = connection.cursor()
    if(creatingTable or fillingTable):
        funcs.main(cursor, connection, bd_table, firstFile, secondFile, creatingTable, fillingTable)

    if(writingBest):
        funcs.findBest(bd_table, cursor, connection)

except (Exception, Error) as error:
    print("Ошибка ", error)

finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")

