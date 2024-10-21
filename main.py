import psycopg2
from psycopg2 import OperationalError
from config import host, username, password, db, port

# Функция устанавливает соединение с БД
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
        print("Соединение с БД PostgreSQL успешно установлено")
    except OperationalError as error:
        print(f"Произошла ошибка при подключении к БД PostgreSQL {error}")
    return connection

connection = create_connection(db, username, password, host, port)

# Определяем функцию, выполняющий запрос
def execute_query(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print('Запрос успешно выполнен')
    except OperationalError as error:
        print(f'Произошла ошибка при выполнении запроса {error}')


create_users_table = """
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL, 
  age INTEGER,
  gender TEXT,
  nationality TEXT
)
"""

execute_query(connection, create_users_table)

# Вставляем данные

users = [("James", 25, "male", "USA"),
    ("Leila", 32, "female", "France"),
    ("Brigitte", 35, "female", "England"),
    ("Mike", 40, "male", "Denmark"),
    ("Elizabeth", 21, "female", "Canada"),
    ]
user_records = ', '.join(['%s'] * len(users))

insert_query = (f'INSERT INTO users (name, age, gender, nationality) VALUES {user_records}')
connection.autocommit = True
cursor = connection.cursor()
cursor.execute(insert_query, users)

# Обновление данных

update_users = """
UPDATE 
    users
SET
    name = 'John'
WHERE 
    nationality = 'USA'
    """
# Вывод данных

def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except OperationalError as error:
        print(f'Произошла ошибка при выводе дынных {error}')

select_users = 'SELECT * FROM users'
# users = execute_read_query(connection, select_users)
execute_query(connection, update_users)
# for user in users:
#     print(user)