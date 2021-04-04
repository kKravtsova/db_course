import psycopg2
from psycopg2 import Error
import pandas as pd
import re

def generateTable(df, table, cur, conn):
    col_array = df.columns
    str_example = df.head(1)
    col_query = ""
    col_q = ""

    for row in str_example.itertuples():
        for value in col_array:
            col_query = col_query + value + " "
            if (type(getattr(row, value)) == str):
                col_query = col_query + "varchar, "
            else:
                col_query = col_query + type(getattr(row, value)).__name__ + ", "
            
    cur.execute(f'CREATE TABLE {table} ({col_query[:-2]})')
    conn.commit()

def fillTable(df, table, cur, conn):
    col_array = df.columns
    
    for row in df.itertuples():
        col_list = ""
        col_values = ""
        for col_name in col_array:
            col_list = col_list + col_name + ", "
            col_values = col_values + "'" + str(getattr(row, col_name)) + "',"
        insert_query = f"INSERT INTO {table} ({col_list[:-2]}) VALUES ({col_values[:-1]})"
        cur.execute(insert_query)
        conn.commit()

def readCsvFile(file):
    # Import CSV
    data = pd.read_csv (file, sep=';', encoding='cp1251', decimal=',')
    df = pd.DataFrame(data, columns= data.columns)
    # Formating for us
    for col in df.columns:
        pattern = re.compile("\S+(Ball100|Ball12|Ball)")
        if(pattern.search(col)):
            df[col].fillna(0, inplace = True)
    df.fillna("No", inplace = True)
    df.replace("\'", "", regex=True, inplace=True)
    return df

def main(cur, conn, table, firstFile, secondFile, creatingTable, fillingTable):
        df_first = readCsvFile(firstFile)
        df_second = readCsvFile(secondFile)

        if(creatingTable):
            try:
                generateTable(df_first, table, cur, conn)
            except (Exception, Error) as error:
                print("Таблица не создана ", error)

        if(fillingTable):
            try:
                fillTable(df_first, table, cur, conn)
                fillTable(df_second, table, cur, conn)
            except (Exception, Error) as error:
                print("Ошибка при заполнении ", error)

        conn.commit()

def findBest(table, cur, conn):
    try:
        select_query = f"SELECT DISTINCT regname FROM {table}"
        cur.execute(select_query)
        unicReg = cur.fetchall()
        for reg in unicReg:
            query = f'''SELECT OUTID, REGNAME, UkrBall100 FROM {table} WHERE UkrBall100=(SELECT MAX(UkrBall100)
                    FROM {table} WHERE REGNAME='{reg[0]}')
                    AND REGNAME='{reg[0]}' AND UkrTestStatus='Зараховано' LIMIT 1'''
            cur.execute(query)
            result = cur.fetchall()
            print(result)
        conn.commit()
    except (Exception, Error) as error:
        print("Ошибка при работе ", error)