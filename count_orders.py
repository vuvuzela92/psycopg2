import pandas as pd
import gspread
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import OperationalError
from sqlalchemy import create_engine

# Подключение к Google Sheets
gc = gspread.service_account(filename='C:/Users/123/Desktop/adv_test/creds.json')
table = gc.open("UNIT 2.0 (tested)")

# Устанавливаем дату и время
today = datetime.now().strftime("%d.%m")
today_formatted = (datetime.now()).strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(1)).strftime('%d.%m')
yesterday_formatted = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
now = (datetime.now()).strftime('%H-%M-%S')
hour = int(datetime.now().strftime('%H'))

# Загружаем данные из Google Sheets и обрабатываем
count_sheet = table.worksheet('Количество заказов').get_all_values()
count_df = pd.DataFrame(count_sheet[1:], columns=count_sheet[0])

# Проверка наличия столбцов с текущей и вчерашней датой
if today in count_df.columns:
    google_tab = count_df[['Артикул', today]]
    google_tab['Артикул'] = google_tab['Артикул'].astype(int)
    google_tab[today] = google_tab[today].replace('', 0)
    google_tab[today] = google_tab[today].fillna(0).infer_objects(copy=False).astype(int)
    google_tab['date'] = today_formatted
    google_tab = google_tab[['date', 'Артикул', today]]
else:
    print(f"Столбец с датой '{today}' отсутствует в DataFrame.")
    google_tab = None

if yesterday in count_df.columns:
    yesterday_google_tab = count_df[['Артикул', yesterday]]
    yesterday_google_tab['Артикул'] = yesterday_google_tab['Артикул'].astype(int)
    yesterday_google_tab[yesterday] = yesterday_google_tab[yesterday].replace('', 0)
    yesterday_google_tab[yesterday] = yesterday_google_tab[yesterday].fillna(0).infer_objects(copy=False).astype(int)
    yesterday_google_tab['date'] = yesterday_formatted
    yesterday_google_tab = yesterday_google_tab[['date', 'Артикул', yesterday]]
else:
    print(f"Столбец с датой '{yesterday}' отсутствует в DataFrame.")
    yesterday_google_tab = None

# Подключение к базе данных
def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print(f"Соединение с БД PostgreSQL успешно установлено в {now}")
    except OperationalError as error:
        print(f"Произошла ошибка при подключении к БД PostgreSQL {error}")
    return connection

# Исполнение SQL запросов
def execute_query(connection, query, data=None):
    cursor = connection.cursor()
    try:
        if data:
            cursor.execute(query, data)
        else:
            cursor.execute(query)
        connection.commit()  # явное подтверждение транзакции
        print(f"Запрос успешно выполнен в {now}")
    except Exception as e:
        connection.rollback()  # откат транзакции в случае ошибки
        print(f"Ошибка выполнения запроса: {e}")
    finally:
        cursor.close()

# Функция на чтение данных из БД
def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as error:
        print(f'Произошла ошибка при выводе данных {error}')

# Получаем таблицу из БД
db_table = 'SELECT * FROM orders_by_hours'
connection = create_connection('vector_db', 'vector_admin', 'skurbick01052023', '192.168.2.57', '5432')
orders_by_hours = execute_read_query(connection, db_table)

# Преобразуем таблицу в датафрейм
try:
    df_db = pd.read_sql(db_table, connection).fillna(0).infer_objects(copy=False)
except Exception as e:
    print(f'Ошибка получения данных из БД {e}')

# Приводим столбец с датой к строковому типу, для дальнейшей обработки
df_db['date'] = df_db['date'].astype(str)
df_db['article_id'] = df_db['article_id'].astype(int)

# Обрабатываем информацию для времени от полуночи до часа ночи
query = f"""INSERT INTO orders_by_hours (date, article_id, hour_{hour-1}_{hour}, sum_count)
    VALUES (%s, %s, %s, %s)"""

# Создание подключения с использованием SQLAlchemy
engine = create_engine(f'postgresql+psycopg2://vector_admin:skurbick01052023@192.168.2.57:5432/vector_db')
connection = create_connection('vector_db', 'vector_admin', 'skurbick01052023', '192.168.2.57', '5432')

for index, row in count_df.iterrows():
    article_id = row['Артикул']
    current_hour_count = row[today]

    if pd.isna(current_hour_count) or current_hour_count == "":
        continue

if hour == 1:
    if google_tab is not None:
        first_hour_concat = pd.concat([df_db, google_tab])
        first_hour_concat['hour_0_1'] = first_hour_concat[today]
        first_hour_concat['sum_count'] = first_hour_concat[today]
        first_hour_concat['article_id'] = first_hour_concat['Артикул']
        first_hour_concat = first_hour_concat.drop(['Артикул', today], axis='columns')
        # Вставка новых данных
        first_hour_concat.to_sql('orders_by_hours', engine, if_exists='append', index=False)

elif hour == 0:
    if yesterday_google_tab is not None:
        # Из таблицы в БД создаем датафрейм с нужными нам колонками
        yesterday_filtred_df = df_db[df_db['date'] == yesterday_formatted]

        # Вычисляем сумму заказов на прошлый час
        yesterday_filtred_df.iloc[:, 2:-1] = yesterday_filtred_df.iloc[:, 2:-1].apply(pd.to_numeric, errors='coerce').fillna(0).astype(float)
        yesterday_filtred_df['sum_count'] = yesterday_filtred_df.iloc[:, 2:-1].sum(axis='columns').fillna(0)

        # Создаем отдельный датафрейм с артикулом и суммой заказов на прошлый час, для дальнейших вычислений
        yesterday_order_for_hour = yesterday_filtred_df[['article_id', 'sum_count']]

        # Объединяем датафреймы с количеством заказов на прошлый час и датафрейм с суммой заказов за день
        yesterday_merged_df = pd.merge(yesterday_google_tab, yesterday_order_for_hour, how='left', left_on='Артикул', right_on='article_id').drop('Артикул', axis='columns')
        yesterday_merged_df[yesterday] = yesterday_merged_df[yesterday].astype(int)

        # Вычисляем количество заказов за крайний час
        yesterday_merged_df['order_count'] = yesterday_merged_df['sum_count'] - yesterday_merged_df[yesterday]

        # Обрабатываем соединенный датафрейм, для дальнейшей загрузки в БД
        yesterday_hour_count_df = yesterday_merged_df[['article_id', 'order_count']]

        # Соединяем обработанный датафрейм из БД с датафреймом с инфой о кол-ве заказов за крайний час
        yesterday_final_df = yesterday_filtred_df.merge(yesterday_hour_count_df, how='left', on='article_id')
        yesterday_final_df[f'hour_23_24'] = yesterday_final_df['order_count']

        # Удаляем лишний столбец, образовавшийся при объединении
        yesterday_final_df = yesterday_final_df.drop('order_count', axis='columns')

        # Пересчитываем сумму заказов
        yesterday_final_df['sum_count'] = yesterday_final_df.iloc[:, 2:-1].sum(axis='columns').fillna(0)

        # Формируем SQL запрос
        yesterday_delete_query = f"""
        DELETE FROM orders_by_hours
        WHERE date = '{yesterday_formatted}';
        """

        # Выгружаем результат в БД
        connection = create_connection('vector_db', 'vector_admin', 'skurbick01052023', '192.168.2.57', '5432')
        execute_query(connection, yesterday_delete_query)
        # Вставка новых данных
        yesterday_final_df.to_sql('orders_by_hours', engine, if_exists='append', index=False)

else:
    # Обработка данных для периодов с 02:00 до 23:00
    # Из таблицы в БД создаем датафрейм с нужными нам колонками
    filtred_df = df_db[df_db['date'] == today_formatted]
    
    # Вычисляем сумму заказов на прошлый час
    filtred_df.iloc[:, 2:-1] = filtred_df.iloc[:, 2:-1].apply(pd.to_numeric, errors='coerce').fillna(0).astype(float)
    filtred_df['sum_count'] = filtred_df.iloc[:, 2:-1].sum(axis='columns').fillna(0)

    # Создаем отдельный датафрейм с артикулом и суммой заказов на прошлый час, для дальнейших вычислений
    order_for_hour = filtred_df[['article_id', 'sum_count']]

    # Объединяем датафреймы с количеством заказов на прошлый час и датафрейм с суммой заказов за день
    merged_df = pd.merge(order_for_hour, google_tab, how='left', left_on='article_id', right_on='Артикул').drop('Артикул', axis='columns')
    merged_df[today] = merged_df[today].astype(float)

    # Вычисляем количество заказов за крайний час
    merged_df['order_count'] = merged_df['sum_count'] - merged_df[today]

    # Обрабатываем соединенный датафрейм, для дальнейшей загрузки в БД
    hour_count_df = merged_df[['article_id', 'order_count']]

    # Соединяем обработанный датафрейм из БД с датафреймом с инфой о кол-ве заказов за крайний час
    final_df = filtred_df.merge(hour_count_df, how='left', on='article_id')
    final_df[f'hour_{hour-1}_{hour}'] = final_df['order_count']
    # Удаляем лишний столбец, образовавшийся при объединении
    final_df = final_df.drop('order_count', axis='columns')

    # Пересчитываем сумму заказов
    final_df['sum_count'] = final_df.iloc[:, 2:-1].sum(axis='columns').fillna(0)

    # Формируем SQL запрос
    delete_query = f"""
    DELETE FROM orders_by_hours
    WHERE date = '{today_formatted}';
    """
    # Выгружаем результат в БД
    connection = create_connection('vector_db', 'vector_admin', 'skurbick01052023', '192.168.2.57', '5432')
    execute_query(connection, delete_query)
    final_df.to_sql('orders_by_hours', engine, if_exists='append', index=False)