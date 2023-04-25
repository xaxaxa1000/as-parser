import requests  # Библиотека для работы с запросами
import json  # Библиотека для работы с json
import psycopg2  # Библиотека для работы с Postgresql
from neo4j import GraphDatabase
import pandas
import random
import time

# database="<DB NAME>"

driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("12345678", "12345678"))


def run_query(query):
    # Используем контекстный менеджер для управления сессией
    with driver.session() as session:
        # Выполняем запрос и возвращаем результат
        return session.run(query)


# Создаем соединение
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="12345"
)

conn.autocommit = True  # чтобы выполнить CREATE DATABASE вне транзакции
cur = conn.cursor()
# Создаем базу данных autonomous_system если она не создана
try:
    cur.execute("CREATE DATABASE autonomous_system")
except Exception as e:
    print(e)

cur.close()
conn.close()

# Создаем таблицу autonomous_system в базе данных autonomous_system
conn = psycopg2.connect(
    host="localhost",
    database="autonomous_system",
    user="postgres",
    password="12345"
)
conn.autocommit = True  # чтобы выполнить CREATE DATABASE вне транзакции
cur = conn.cursor()
try:
    cur.execute("DROP TABLE autonomous_system")
    cur.execute("DROP TABLE all_autonomous_system")
    cur.execute("DROP TABLE connection")
except Exception as e:
    print(e)
# timestamp  without time zone
try:
    cur.execute("""CREATE TABLE autonomous_system(
    id SERIAL PRIMARY KEY,
    asn INTEGER,
    prefix CHARACTER VARYING(30),
    source_id CHARACTER VARYING(43),
    ts timestamp NOT NULL,
    path_to_asn INTEGER []
    );""")

    cur.execute("""CREATE TABLE all_autonomous_system(
    id SERIAL PRIMARY KEY,
    asn BIGINT
    );""")

    cur.execute("""CREATE TABLE connection(
            asn1 BIGINT,
            asn2 BIGINT
            );""")
    # cur.execute("""CREATE TABLE connection(
    #     id SERIAL PRIMARY KEY,
    #     asn1 BIGINT,
    #     asn2 BIGINT
    #     );""")
except Exception as e:
    print(e)

# РАСКОМЕНДИТЬ ТУТ
insert_connection_query = """ INSERT INTO connection (asn1,asn2) VALUES (%s,%s)"""
insert_all_as_query = """ INSERT INTO all_autonomous_system (asn) VALUES (%s)"""
insert_query = """ INSERT INTO autonomous_system (asn, prefix,source_id,ts, path_to_asn) VALUES (%s,%s,%s,%s,%s)"""
# Getting a list of all AS
url_all_as = 'https://stat-ui.stat.ripe.net/data/ris-asns/data.json?list_asns=true'
response = requests.get(url_all_as)
all_as_data = response.json()
#ЧТОБЫ ЗАПОЛНЯТЬ ТАБЛИЦУ all_autonomous_system раскомендить тут
for i in range(len(all_as_data['data']['asns'])):
    asn = all_as_data['data']['asns'][i]
    # Создается запрос
    cur.execute(insert_all_as_query, [asn])
    # Выполняется соединение
    conn.commit()
    print("добавлена "+str(i)+ " as из "+str(all_as_data['data']['counts']['total']))
connection_data = set()
for i in range(len(all_as_data['data']['asns'])):
    asn = all_as_data['data']['asns'][i]
    print("AS" + str(asn))
    url_some_as = 'https://stat.ripe.net/data/bgp-state/data.json?resource=AS' + str(asn)
    # print(url_some_as)

    # Выполняется запрос
    response = requests.get(url_some_as,timeout=5)
    # Ответ запроса преобразуется в json
    try:
        data = response.json()
    except Exception as e:
        print(e)
    # ASN
    current_as = data['data']['resource']

    # Время записи
    timestamp = data['time']
    # print(timestamp)
    # if(connection_data.empty==False):
    #    connection_data.drop_duplicates(subset=["path_from", "path_to"], inplace=True)
    connection_data.clear()
    for j in range(len(data['data']['bgp_state'])):
        # ТЕСТИРУЮ ТУТ--------------------------------------------------------------
        # print("длина маршрута "+str(len(data['data']['bgp_state'][j]['path'])))
        path_length = len(data['data']['bgp_state'][j]['path'])
        # path_to = data['data']['bgp_state'][j]['path'][path_length - 1]
        # path_from = data['data']['bgp_state'][j]['path'][path_length - 2]

        try:
            index_path_to = data['data']['bgp_state'][j]['path'].index(int(current_as))
            path_to = data['data']['bgp_state'][j]['path'][index_path_to]
            path_from = data['data']['bgp_state'][j]['path'][index_path_to - 1]
            if (path_to < path_from):
                connection_data.add((path_to, path_from))
                # cur.execute(insert_connection_query, [path_to,path_from])
            if (path_to > path_from):
                connection_data.add((path_to, path_from))
                # cur.execute(insert_connection_query, [path_from,path_to])
        except Exception as e:
            print(e)
        #path_to = data['data']['bgp_state'][j]['path'][index_path_to]
        #path_from = data['data']['bgp_state'][j]['path'][index_path_to-1]

        #print(str(path_to)+"       \n"+str(path_from))

    try:
        cur.executemany(insert_connection_query, connection_data)
    except Exception as e:
        print(e)


    # ТЕСТИРУЮ ТУТ--------------------------------------------------------------
    # ЧТОБЫ ЗАПОЛНИТЬ ТАБЛИЦУ autonomous_system раскомендить тут
    # #Префикс
    # prefix = data['data']['bgp_state'][j]['target_prefix']
    # #Source id
    # source_id = data['data']['bgp_state'][j]['source_id']
    # #Путь
    # path = data['data']['bgp_state'][j]['path']
    # #Путь преобразуется в массив для записи в бд
    # path_array = "{" + ", ".join(str(x) for x in path) + "}"
    # #Формируется запись в postgresql
    # record_to_insert = (current_as, prefix,source_id, timestamp, path_array)
    # #Создается запрос
    # cur.execute(insert_query, record_to_insert)
    # #Выполняется соединение
    # conn.commit()
    # count = cur.rowcount
    # #print(count, "Запись успешно добавлена в таблицу users")

cur.close()
conn.close()