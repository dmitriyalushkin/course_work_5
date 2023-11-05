import psycopg2
import requests


def get_vacancies(vacancies):
    """Получение данных вакансий по API"""

    vacancies_data = []
    for employer_id in vacancies:
        params = {
            'per_page': 10
        }
        url = f"https://api.hh.ru/vacancies?employer_id={employer_id}"
        data_vacancies = requests.get(url, params=params).json()['items']
        for item in data_vacancies:
            salary = item['salary']
            vacancy_id = item['id']
            vacancies_name = item['name']
            salary_from = 0 if salary is None or salary['from'] is None else salary['from']
            salary_to = 0 if salary is None or salary['to'] is None else salary['to']
            requirement = item['snippet']['requirement']
            vacancies_url = item['alternate_url']
            employer_id = item['employer']['id']

            vacancies_data.append([vacancy_id, vacancies_name, salary_from,
                                   salary_to, requirement, vacancies_url, employer_id])
    return vacancies_data


def get_employer(employers_id):
    """Получение данных о работодателей  по API"""

    employer_data = []
    data = []

    for employer_id in employers_id:
        params = {
            'per_page': 10,
            'open_vacancies': True
        }
        url = f"https://api.hh.ru/employers?employer_id={employer_id}"
        data_vacancies = requests.get(url, params=params).json()['items']
        # data.append(data_vacancies)
        for item in data_vacancies:

            employer_id = item['id']
            company_name = item['name']
            open_vacancies = item['open_vacancies']
            employer_data.append([employer_id, company_name, open_vacancies])
    return employer_data


def create_table(database_name, params):
    """Создание БД, созданение таблиц"""

    with psycopg2.connect(db_name=database_name, **params) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"DROP database IF EXISTS {database_name}")
            cur.execute(f"CREATE database {database_name}")
            cur.execute("""
                        CREATE TABLE employers (
                        employer_id INTEGER PRIMARY KEY,
                        company_name varchar(255),
                        open_vacancies INTEGER
                        )""")

            cur.execute("""
                        CREATE TABLE vacancies (
                        vacancy_id INTEGER PRIMARY KEY,
                        vacancies_name varchar(255),
                        salary INTEGER,
                        salary_from INTEGER,
                        salary_to INTEGER,
                        requirement TEXT,
                        vacancies_url TEXT,
                        employer_id INTEGER REFERENCES employers(employer_id),
                        )""")
        conn.commit()
        conn.close()

def save_employers_to_db(data, database_name, params):
    """Заполнение базы данных компании"""

    with psycopg2.connect(db_name=database_name, **params) as conn:
        with conn.cursor() as cur:
            for employer in data:
                query = 'INSERT INTO employers(employer_id, company_name, open_vacancies) ' \
                        'VALUES (%s, %s, %s)'
                cur.execute(query, employer)
        conn.commit()
        conn.close()

def save_vacancies_to_db(data, database_name, params):
    """Заполнение базы данных вакансии"""

    with psycopg2.connect(db_name=database_name, **params) as conn:
        with conn.cursor() as cur:
            for vacancy in data:
                query = 'INSERT INTO vacancies(vacancy_id, vacancies_name, salary, salary_from,' \
                        'salary_to, requirement, vacancies_url, employer_id) ' \
                        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
                cur.execute(query, vacancy)
        conn.commit()
        conn.close()

# print(get_employer([10]))