import argparse
import os
from datetime import datetime
from typing import Optional

import psycopg2
from dotenv import load_dotenv


def add_data_mart(standard_n1_0: float, standard_n1_1: float, standard_n1_2: float, date: Optional[str] = None) -> None:
    # расчет данных по указанной дате
    if date != 'None' and date:
        cur.execute('SELECT * FROM dwh.common_data WHERE "date" = %s', (date,))
        rows = cur.fetchall()
    # расчет показателей на все даты, указанные в БД
    else:
        cur.execute('SELECT * FROM dwh.common_data')
        rows = cur.fetchall()

    cur.execute('SELECT MAX(date) FROM data_mart.params')
    last_timestamp = cur.fetchone()[0]

    for row in rows:
        bank_total_capital = row[5]
        if not bank_total_capital:
            bank_total_capital = 0
        client_deposits_total = row[1]
        company_deposits_total = row[2]
        bank_total_liabilities = row[3]
        bank_total_assets = row[4]
        bank_total_capital = bank_total_capital*100
        date = row[6]

        # Проверка что нет таких же объектов в data_mart и добавление в data_mart
        if not last_timestamp or date not in dates:

            # Проверка что есть все данные для вычисления коэффициентов
            if any(var is None for var in [client_deposits_total, company_deposits_total,
                                           bank_total_capital, bank_total_assets, bank_total_liabilities]):
                n1_0 = None
                n1_1 = None
                n1_2 = None
                timestamp = datetime.now()
            else:
                deposits_total = client_deposits_total + company_deposits_total
                n1_0 = client_deposits_total/bank_total_capital
                n1_1 = (bank_total_capital/100)/(deposits_total+bank_total_assets+bank_total_liabilities)
                n1_2 = client_deposits_total/deposits_total
                timestamp = datetime.now()

            # Добавление данных в data_mart.params
            cur.execute('INSERT INTO data_mart.params (n1_0, standard_n1_0, n1_1, standard_n1_1, '
                        'n1_2, standard_n1_2, date, timestamp)'
                        ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s)', (n1_0, standard_n1_0, n1_1, standard_n1_1, n1_2,
                                                                     standard_n1_2, date, timestamp))


if __name__ == '__main__':
    # Использование переменных окружения
    load_dotenv()

    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")

    # Использование парсера
    parser = argparse.ArgumentParser()
    parser.add_argument('standard_n1_0',
                        type=float,
                        nargs='?',
                        help='параметр, установленный ЦБ',
                        default=0.08)

    parser.add_argument('standard_n1_1',
                        type=float,
                        nargs='?',
                        help='параметр, установленный ЦБ',
                        default=0.045)

    parser.add_argument('standard_n1_2',
                        type=float,
                        nargs='?',
                        help='параметр, установленный ЦБ',
                        default=0.06)

    parser.add_argument('date',
                        nargs='?',
                        help='Дата, на которую будет загружены параметры. Формат 2024-11-01',
                        default=datetime.now().strftime('%Y-%m-%d'))

    args = parser.parse_args()

    # Подключение к базе данных
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host
    )
    cur = conn.cursor()

    # Получаем список дат, уже существующих в common_data, чтобы не задваивать информацию
    cur.execute("SELECT date FROM data_mart.params")
    dates = [row[0] for row in cur.fetchall()]

    add_data_mart(args.standard_n1_0, args.standard_n1_1, args.standard_n1_2, args.date)
    if args.date == 'None':
        print(f"Все отсутствующие данные загружены в витрину. Обновите PowerBi")
    else:
        print(f"Данные на {args.date} загружены в витрину. Обновите PowerBi")

    # Сохраняем изменения
    conn.commit()

    # Закрываем соединение
    cur.close()
    conn.close()
