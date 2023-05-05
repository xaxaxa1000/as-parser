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

countries = ["AU", "AT", "AZ", "AX", "AL", "DZ", "VI", "AS", "AI", "AO", "AD", "AQ", "AG", "AR", "AM", "AW", "AF",
             "BS", "BD", "BB", "BH", "BZ", "BY", "BE", "BJ", "BM", "BG", "BO", "BQ", "BA", "BW", "BR", "IO", "VG",
             "BN", "BF", "BI", "BT", "VU", "VA", "GB", "HU", "VE", "UM", "TL", "VN", "GA", "HT", "GY", "GM", "GH",
             "GP", "GT", "GF", "GN", "GW", "DE", "GG", "GI", "HN", "HK", "GD", "GL", "GR", "GE", "GU", "DK", "JE",
             "DJ", "DM", "DO", "CD", "EG", "ZM", "EH", "ZW", "IL", "IN", "ID", "JO", "IQ", "IR", "IE", "IS",
             "ES", "IT", "YE", "CV", "KZ", "KY", "KH", "CM", "CA", "QA", "KE", "CY", "KG", "KI", "TW", "KP", "CN",
             "CC", "CO", "KM", "CR", "CI", "CU", "KW", "CW", "LA", "LV", "LS", "LR", "LB", "LY", "LT", "LI", "LU",
             "MU", "MR", "MG", "YT", "MO", "MK", "MW", "MY", "ML", "MV", "MT", "MA", "MQ", "MH", "MX", "FM", "MZ",
             "MD", "MC", "MN", "MS", "MM", "NA", "NR", "NP", "NE", "NG", "NL", "NI", "NU", "NZ", "NC", "NO", "AE",
             "OM", "BV", "IM", "CK", "NF", "CX", "PN", "SH", "PK", "PW", "PS", "PA", "PG", "PY", "PE", "PL", "PT",
             "PR", "CG", "KR", "RE", "RU", "RW", "RO", "SV", "WS", "SM", "ST", "SA", "SZ", "MP", "SC", "BL", "MF",
             "PM", "SN", "VC", "KN", "LC", "RS", "SG", "SX", "SY", "SK", "SI", "SB", "SO", "SD", "SR", "US", "SL",
             "TJ", "TH", "TZ", "TC", "TG", "TK", "TO", "TT", "TV", "TN", "TM", "TR", "UG", "UZ", "UA", "WF", "UY",
             "FO", "FJ", "PH", "FI", "FK", "FR", "PF", "TF", "HM", "HR", "CF", "TD", "ME", "CZ", "CL", "CH", "SE",
             "SJ", "LK", "EC", "GQ", "ER", "EE", "ET", "ZA", "GS", "SS", "JM", "JP"]
print("1. Парсить все автономные системы(~1 min)\n2. Парсить все автономные системы по странам(~4 min)\n3. Парсить все автономные системы по странам с координатами(~3 min)\n4. Парсить соединения (~ОЧЕНЬ МНОГО min)")
while True:
    answer = input("\nВыберите пункт: ")
    if answer == "1":
        conn = psycopg2.connect(
            host="localhost",
            database="autonomous_system",
            user="postgres",
            password="12345"
        )
        conn.autocommit = True  # чтобы выполнить CREATE DATABASE вне транзакции
        cur = conn.cursor()
        # Удаляется существующая таблица
        try:
            cur.execute("DROP TABLE all_autonomous_system")
        except Exception as e:
            print(e)
        # Создается новая таблица
        try:
            cur.execute("""CREATE TABLE all_autonomous_system(
            id SERIAL PRIMARY KEY,
            asn BIGINT
            );""")
        except Exception as e:
            print(e)

        # List в котором хранятся все номера as
        all_as_list = list()
        # Insert запрос для таблицы all_autonomous_system
        insert_all_as_query = """ INSERT INTO all_autonomous_system (asn) VALUES (%s)"""
        # URL
        url_all_as = 'https://stat-ui.stat.ripe.net/data/ris-asns/data.json?list_asns=true'
        # Выполнение запроса
        response = requests.get(url_all_as)
        # Преобразование запроса в json
        all_as_data = response.json()

        # ЧТОБЫ ЗАПОЛНЯТЬ ТАБЛИЦУ all_autonomous_system раскомендить тут
        for i in range(len(all_as_data['data']['asns'])):
            all_as_list.append(all_as_data['data']['asns'][i])
        print("\nВсе автономные системы считаны и добавляются в таблицу all_autonomous_system")
        cur.executemany(insert_all_as_query, [(asn,) for asn in all_as_list])
        print("\nВсе вершины успешно добавлены в таблицу all_autonomous_system")
        conn.commit()
        cur.close()
        conn.close()

    elif answer == "2":
        conn = psycopg2.connect(
            host="localhost",
            database="autonomous_system",
            user="postgres",
            password="12345"
        )
        conn.autocommit = True  # чтобы выполнить CREATE DATABASE вне транзакции
        cur = conn.cursor()
        # Удаляется существующая таблица
        try:
            cur.execute("DROP TABLE all_autonomous_system_with_country")
        except Exception as e:
            print(e)
        # Создается новая таблица
        try:
            cur.execute("""CREATE TABLE all_autonomous_system_with_country(
                    id SERIAL PRIMARY KEY,
                    asn BIGINT,
                    country CHARACTER VARYING(3)
                    );""")
        except Exception as e:
            print(e)

        insert_all_as_query = """ INSERT INTO all_autonomous_system_with_country (asn,country) VALUES (%s,%s)"""
        country_as_set = set()
        count=1
        for country in countries:
            url="https://stat.ripe.net/data/country-resource-list/data.json?resource="+country
            response = requests.get(url)
            all_as_data = response.json()
            country_as_set.clear()
            #print("Добавлено 0 стран из %s"% len(countries),end="")
            for i in range(len(all_as_data['data']['resources']['asn'])):
                country_as_set.add((all_as_data['data']['resources']['asn'][i],country))

            cur.executemany(insert_all_as_query, country_as_set)
            print("\rДобавлено %s стран из %s" % (count, len(countries)), end="")
            count=count+1
            conn.commit()

        cur.close()
        conn.close()

    elif answer == "3":
        conn = psycopg2.connect(
            host="localhost",
            database="autonomous_system",
            user="postgres",
            password="12345"
        )
        conn.autocommit = True  # чтобы выполнить CREATE DATABASE вне транзакции
        cur = conn.cursor()

        try:
            cur.execute("DROP TABLE all_as_with_country_and_coord")
        except Exception as e:
            print(e)
            # Создается новая таблица
        try:
            cur.execute("""CREATE TABLE all_as_with_country_and_coord(
                id SERIAL PRIMARY KEY,
                asn BIGINT,
                country CHARACTER VARYING(3),
                latitude REAL,
                longitude REAL
                );""")
        except Exception as e:
            print(e)

        url = "https://stat.ripe.net/data/atlas-probes/data.json?resource=GA"
        insert_all_as_query = """ INSERT INTO all_as_with_country_and_coord (asn,country,latitude,longitude) VALUES (%s,%s,%s,%s)"""
        country_as_set = set()
        count = 1
        for country in countries:
            url = "https://stat.ripe.net/data/atlas-probes/data.json?resource=" + country
            #url = "https://stat.ripe.net/data/atlas-probes/data.json?resource=GA"
            response = requests.get(url)
            all_as_data = response.json()
            country_as_set.clear()
            for i in range(len(all_as_data['data']['probes'])):
                if(all_as_data['data']['probes'][i]['asn_v4']):
                    asn=all_as_data['data']['probes'][i]['asn_v4']
                    latitude=all_as_data['data']['probes'][i]['latitude']
                    longitude=all_as_data['data']['probes'][i]['longitude']
                    country_as_set.add((asn, country,latitude,longitude))

            cur.executemany(insert_all_as_query, country_as_set)
            print("\rДобавлено %s стран из %s" % (count, len(countries)), end="")
            count = count + 1
            conn.commit()

        cur.close()
        conn.close()
                    #asn=all_as_data['data']['probes']['asn'][i]
                    #country_as_set.add((all_as_data['data']['resources']['asn'][i], country))
    elif answer == "4":
        conn = psycopg2.connect(
            host="localhost",
            database="autonomous_system",
            user="postgres",
            password="12345"
        )
        conn.autocommit = True  # чтобы выполнить CREATE DATABASE вне транзакции
        cur = conn.cursor()
        try:
            cur.execute("DROP TABLE connection")
        except Exception as e:
            print(e)
        try:
            cur.execute("""CREATE TABLE connection(
                    asn1 BIGINT,
                    asn2 BIGINT
                    );""")
        except Exception as e:
            print(e)

        connection_data = set()

        insert_connection_query = """ INSERT INTO connection (asn1,asn2) VALUES (%s,%s)"""
        select_query="select asn from all_as_with_country_and_coord"
        cur.execute(select_query)
        asns = cur.fetchall()
        count=1
        for asn in asns:
            #print(asn[0])
            #asn = all_as_data['data']['asns'][i]
            url_some_as = 'https://stat.ripe.net/data/bgp-state/data.json?resource=AS' + str(asn[0])
            # print(url_some_as)
            # Выполняется запрос
            response = requests.get(url_some_as)
            # Ответ запроса преобразуется в json
            try:
                data = response.json()
            except Exception as e:
                print(e)

            connection_data.clear()
            for j in range(len(data['data']['bgp_state'])):
                path_length = len(data['data']['bgp_state'][j]['path'])
                try:
                    for t in range(len(data['data']['bgp_state'][j]['path'])):
                        path_from = data['data']['bgp_state'][j]['path'][t - 1]
                        path_to = data['data']['bgp_state'][j]['path'][t]
                        if (path_to < path_from):
                            connection_data.add((path_to, path_from))
                        if (path_to > path_from):
                            connection_data.add((path_from, path_to))
                except Exception as e:
                    print(e)

            try:
                cur.executemany(insert_connection_query, connection_data)
                cur.execute(
                    """DELETE FROM connection WHERE ctid NOT IN (SELECT max(ctid) FROM connection GROUP BY connection.*);""")
            except Exception as e:
                print(e)
            print("Добавлено связей " + str(count) + "/" + str(len(asns)) + " AS" + str(asn[0]))
            count=count+1



    else:
        print("wrong input")

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
# ЧТОБЫ ЗАПОЛНЯТЬ ТАБЛИЦУ all_autonomous_system раскомендить тут
for i in range(len(all_as_data['data']['asns'])):
    asn = all_as_data['data']['asns'][i]
    # Создается запрос
    cur.execute(insert_all_as_query, [asn])
    # Выполняется соединение
    conn.commit()
    if i % 1000 == 0:
        print("Добавлено вершин " + str(i) + "/" + str(all_as_data['data']['counts']['total']))

connection_data = set()
for i in range(len(all_as_data['data']['asns'])):
    asn = all_as_data['data']['asns'][i]
    url_some_as = 'https://stat.ripe.net/data/bgp-state/data.json?resource=AS' + str(asn)
    # print(url_some_as)

    # Выполняется запрос
    response = requests.get(url_some_as)
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
            for t in range(len(data['data']['bgp_state'][j]['path'])):
                # index_path_to = data['data']['bgp_state'][j]['path'].index(int(current_as))
                # path_to = data['data']['bgp_state'][j]['path'][index_path_to]
                # path_from = data['data']['bgp_state'][j]['path'][index_path_to - 1]
                path_from = data['data']['bgp_state'][j]['path'][t - 1]
                path_to = data['data']['bgp_state'][j]['path'][t]
                if (path_to < path_from):
                    connection_data.add((path_to, path_from))
                # cur.execute(insert_connection_query, [path_to,path_from])
                if (path_to > path_from):
                    connection_data.add((path_from, path_to))
                # cur.execute(insert_connection_query, [path_from,path_to])
        except Exception as e:
            print(e)
        # path_to = data['data']['bgp_state'][j]['path'][index_path_to]
        # path_from = data['data']['bgp_state'][j]['path'][index_path_to-1]

        # print(str(path_to)+"       \n"+str(path_from))

    try:
        cur.executemany(insert_connection_query, connection_data)
        cur.execute(
            """DELETE FROM connection WHERE ctid NOT IN (SELECT max(ctid) FROM connection GROUP BY connection.*);""")
    except Exception as e:
        print(e)
    print("Добавлено связей " + str(i) + "/" + str(len(all_as_data['data']['asns'])) + " AS" + str(asn))

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
