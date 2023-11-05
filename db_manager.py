import psycopg2
from config import config


class DBManager:
    '''Класс для подключения к БД'''


    def get_companies_and_vacancies_count(self):
        '''Метод получает список всех компаний и
        количество вакансий у каждой компании'''

        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT company_name FROM employees count(vacancies.*) "
                        f"JOIN vacancies USING (employer_id) "
                        f"GROUP BY employees.company_name")
            result = cur.fetchall()
        conn.commit()
        conn.close()
        return result


    def get_all_vacancies(self):
        '''Метод получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию'''
        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT employees.company_name, vacancies.vacancies_name, "
                            f"vacancies.salary, vacancies_url"
                            f"FROM employees"
                            f"JOIN vacancies USING (employer_id)")
                result = cur.fetchall()
            conn.commit()
            conn.close()
            return result


    def get_avg_salary(self):
        '''Метод получает среднюю зарплату по вакансиям'''
        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT AVG(salary) as avg_salary FROM vacancies ")
                result = cur.fetchall()
            conn.commit()
            conn.close()
            return result


    def get_vacancies_with_higher_salary(self):
        '''Метод получает список всех вакансий,
        у которых зарплата выше средней по всем вакансиям'''
        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM vacancies "
                            f"WHERE salary > (SELECT AVG(salary) FROM vacancies) ")
                result = cur.fetchall()
            conn.commit()
            conn.close()
            return result


    def get_vacancies_with_keyword(self, keyword):
        '''Метод получает список всех вакансий,
        в названии которых содержатся переданные в метод слова'''
        with psycopg2.connect(**config()) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM vacancies "
                            f"WHERE vacancies_name LIKE('%{keyword}%') ")
                result = cur.fetchall()
            conn.commit()
            conn.close()
            return result



# dbmanager = DBManager()
# print(dbmanager.get_companies_and_vacancies_count(15))