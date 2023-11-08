import psycopg2
import requests
from config import config


def get_vacancies(employers_id):
    """Получение данных вакансий по API"""

    vacancies_data = []
    for employer_id in employers_id:
        params = {
            'per_page': 10
        }
        url = f"https://api.hh.ru/vacancies?employer_id={employer_id}"
        data_vacancies = requests.get(url, params=params).json()
        data_vacancies = data_vacancies.get('items')
        if data_vacancies:

            for item in data_vacancies:
                # salary = item['salary']
                vacancy_id = item['id']
                vacancies_name = item['name']
                salary_from = 0 if item['salary'] is None or item['salary']['from'] is None else item['salary']['from']
                salary_to = 0 if item['salary'] is None or item['salary']['to'] is None else item['salary']['to']
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
        url = f"https://api.hh.ru/employers/{employer_id}"
        data_vacancies = requests.get(url, params=params).json()
        # data.append(data_vacancies)
        # for item in data_vacancies:

        employer_id = data_vacancies['id']
        company_name = data_vacancies['name']
        open_vacancies = data_vacancies['vacancies_url']
        employer_data.append([employer_id, company_name, open_vacancies])
    return employer_data


def create_table():
    """Создание БД, созданение таблиц"""

    conn = psycopg2.connect(host="localhost", database="postgres",
                            user="postgres", password="12345")
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP DATABASE IF EXISTS course_work_5")
    cur.execute("CREATE DATABASE course_work_5")

    conn.close()

    conn = psycopg2.connect(host="localhost", database="course_work_5",
                            user="postgres", password="12345")
    with conn.cursor() as cur:
        cur.execute("""
                    CREATE TABLE employers (
                    employer_id SERIAL PRIMARY KEY,
                    company_name varchar(255),
                    open_vacancies INTEGER
                    )""")

        cur.execute("""
                    CREATE TABLE vacancies (
                    vacancy_id SERIAL PRIMARY KEY,
                    vacancies_name varchar(255),
                    salary_from INTEGER,
                    salary_to INTEGER,
                    requirement TEXT,
                    vacancies_url TEXT,
                    employer_id INTEGER REFERENCES employers(employer_id)
                    )""")
    conn.commit()
    conn.close()


def save_employers_to_db(employers_list):
    """Заполнение базы данных компании"""

    with psycopg2.connect(host="localhost", database="course_work_5",
                            user="postgres", password="12345") as conn:
        with conn.cursor() as cur:
            # cur.execute(f'TRUNCATE TABLE employers, vacancies RESTART IDENTITY;')
            for employer in employers_list:
                employer_list = get_employer(employer)
                cur.execute('INSERT INTO employers (employer_id, company_name, open_vacancies) '
                            'VALUES (%s, %s, %s)', (employer_list[0],
                            employer_list[1], employer_list[2]))

        conn.commit()



def save_vacancies_to_db(employers_list):
    """Заполнение базы данных вакансии"""

    with psycopg2.connect(host="localhost", database="course_work_5",
                            user="postgres", password="12345") as conn:
        with conn.cursor() as cur:
            # cur.execute(f'TRUNCATE TABLE employers, vacancies RESTART IDENTITY;')
            for employer in employers_list:
                vacancy_list = get_vacancies(employer)
                for v in vacancy_list:
                    cur.execute('INSERT INTO vacancies (vacancy_id, vacancies_name, '
                    'salary_from, salary_to, requirement, '
                    'vacancies_url, employer_id) '
                    'VALUES (%s, %s, %s, %s, %s, %s, %s)',
                    (v[0], v[1], v[2], v[3], v[4], v[5], v[6]))

        conn.commit()


# print(save_employers_to_db([1740, 15478, 8620, 3529, 78638, 4006, 4504679, 561525, 64174, 8642172, 3785152, 4934, 205152, 3776, 4394]))