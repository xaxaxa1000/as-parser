import requests # Библиотека для работы с запросами
import json # Библиотека для работы с json
import psycopg2 # Библиотека для работы с Postgresql

# Создаем соединение
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="12345"
)

conn.autocommit = True # чтобы выполнить CREATE DATABASE вне транзакции
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
conn.autocommit = True # чтобы выполнить CREATE DATABASE вне транзакции
cur = conn.cursor()
try:
    cur.execute("DROP TABLE autonomous_system")
except Exception as e:
    print(e)
#timestamp  without time zone
try:
    cur.execute("""CREATE TABLE autonomous_system(
    id SERIAL PRIMARY KEY,
    asn INTEGER,
    prefix CHARACTER VARYING(30),
    source_id CHARACTER VARYING(43),
    ts timestamp NOT NULL,
    path_to_asn INTEGER []
);""")

except Exception as e:
    print(e)


insert_query = """ INSERT INTO autonomous_system (asn, prefix,source_id,ts, path_to_asn) VALUES (%s,%s,%s,%s,%s)"""
# Getting a list of all AS
url_all_as = 'https://stat-ui.stat.ripe.net/data/ris-asns/data.json?list_asns=true'
response = requests.get(url_all_as)
all_as_data = response.json()
for i in range(len(all_as_data['data']['asns'])):
    asn = all_as_data['data']['asns'][i]
    #print(asn)
    url_some_as = 'https://stat.ripe.net/data/bgp-state/data.json?resource=AS'+str(asn)
    #print(url_some_as)

    #url = 'https://stat.ripe.net/data/bgp-state/data.json?resource=AS2002'
    #Выполняется запрос
    response = requests.get(url_some_as)
    #Ответ запроса преобразуется в json
    data = response.json()
    #ASN
    current_as = data['data']['resource']
    #Время записи
    timestamp=data['time']
    print(timestamp)
    for j in range(len(data['data']['bgp_state'])):
        #Префикс
        prefix = data['data']['bgp_state'][j]['target_prefix']
        #Source id
        source_id = data['data']['bgp_state'][j]['source_id']
        #Путь
        path = data['data']['bgp_state'][j]['path']
        #Путь преобразуется в массив для записи в бд
        path_array = "{" + ", ".join(str(x) for x in path) + "}"
        #Формируется запись
        record_to_insert = (current_as, prefix,source_id, timestamp, path_array)
        #Создается запрос
        cur.execute(insert_query, record_to_insert)
        #Выполняется соединение
        conn.commit()
        count = cur.rowcount
        print(count, "Запись успешно добавлена в таблицу users")




#cur.execute("""INSERT INTO autonomous_system VALUES('2002', '192.168.111.1/24', s');""")
cur.close()
conn.close()



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
