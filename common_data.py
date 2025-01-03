import argparse
import os
from datetime import datetime, timedelta
from typing import Optional, Tuple

import psycopg2
from dotenv import load_dotenv


def create_common_data(date: str) -> None:
    # Проверка есть ли уже в базе указанная дата, если есть, то загрузка данных на эту дату не будет производиться
    date_obj = datetime.strptime(date, "%Y-%m-%d").date()
    if date_obj not in dates:
        client_deposits_total, company_deposits_total = sum_deposits(date)
        bank_total_liabilities = sum_liabilities(date)
        bank_total_assets = sum_assests(date)
        bank_total_capital = sum_capital(date)

        cur.execute("""
                    INSERT INTO dwh.common_data (client_deposits_total, company_deposits_total,
                     bank_total_liabilities, bank_total_assets, bank_total_capital, date) VALUES (%s, %s, %s, %s, %s, %s)""",
                    (client_deposits_total, company_deposits_total, bank_total_liabilities, bank_total_assets,
                     bank_total_capital, date))


def sum_deposits(date: str) -> Tuple[float, float]:
    # рассчитываем сумму депозитов клиентов, которые открыты и не закрыты на указанную дату
    cur.execute("""
            SELECT COALESCE(SUM(deposit_amount), 0) 
            FROM dwh.deposits_clients
            WHERE opening_date < %s 
            AND closing_date > %s
        """, (date, date))
    client_deposits_total = cur.fetchone()[0]

    # рассчитываем сумму депозитов компаний, которые открыты и не закрыты на указанную дату
    cur.execute("""
                SELECT COALESCE(SUM(deposit_amount), 0) 
                FROM dwh.deposits_companies
                WHERE opening_date < %s 
                AND closing_date > %s
            """, (date, date))
    company_deposits_total = cur.fetchone()[0]
    return client_deposits_total, company_deposits_total


def sum_liabilities(date: str) -> Optional[float]:
    # определяем дату, на которую будет производится поиск данных
    search_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1)
    # идем по циклу с проверкой на какую дату из диапазона delta_days найдутся данные по пассивам банка
    for i in range(delta_days):
        cur.execute("""
                WITH closest_date AS (
                    SELECT *
                    FROM dwh.control_liabilities
                    WHERE timestamp_column::date = %s
                    ORDER BY timestamp_column
                    LIMIT 1
                )
                SELECT * FROM closest_date
            """, (search_date.strftime('%Y-%m-%d'),))
        liability = cur.fetchone()

        # присваиваем переменным соответствующие значения из найденного объекта
        if liability:
            financial_instruments_debts = liability[1]
            securities_obligations = liability[2]
            reporting_data = liability[3]
            invoices_to_pay = liability[4]
            funds_in_accounts = liability[5]

            # расчет суммарных пассивов банка на указанную дату
            bank_total_liabilities = financial_instruments_debts + securities_obligations + reporting_data + \
                                     invoices_to_pay + funds_in_accounts
            return bank_total_liabilities

        search_date -= timedelta(days=1)  # Уменьшаем дату поиска на 1 день, если данные не найдены для текущей даты

    return None


def sum_assests(date: str) -> Optional[float]:
    # определяем дату, на которую будет производится поиск данных
    search_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1)
    # идем по циклу с проверкой на какую дату из диапазона delta_days найдутся данные по активам банка
    for i in range(delta_days):
        cur.execute("""
                    WITH closest_date AS (
                        SELECT *
                        FROM dwh.general_assets
                        WHERE timestamp_column::date = %s
                        ORDER BY timestamp_column
                        LIMIT 1
                    )
                    SELECT * FROM closest_date
                """, (search_date.strftime('%Y-%m-%d'),))
        assets = cur.fetchone()

        # присваиваем переменным соответствующие значения из найденного объекта
        if assets:
            securities = assets[1]
            real_estate = assets[2]
            financial_reports = assets[3]
            credit_facilities = assets[4]
            machinery = assets[5]
            debts = assets[6]
            equipment = assets[7]

            # расчет суммарных активов банка на указанную дату
            bank_total_assets = securities + real_estate + financial_reports + credit_facilities + machinery + \
                           debts + equipment
            return bank_total_assets

        search_date -= timedelta(days=1)  # Уменьшаем дату поиска на 1 день, если данные не найдены для текущей даты

    return None


def sum_capital(date: str) -> Optional[float]:
    # определяем дату, на которую будет производится поиск данных
    search_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1)
    # идем по циклу с проверкой на какую дату из диапазона delta_days найдутся данные по капиталу банка
    for i in range(delta_days):
        cur.execute("""
                    WITH closest_date AS (
                        SELECT *
                        FROM dwh.capital
                        WHERE timestamp_column::date = %s
                        ORDER BY timestamp_column
                        LIMIT 1
                    )
                    SELECT * FROM closest_date
                """, (search_date.strftime('%Y-%m-%d'),))
        capital = cur.fetchone()

        # присваиваем переменным соответствующие значения из найденного объекта
        if capital:
            reserve_fund = capital[1]
            equity_capital = capital[2]
            accumulated_earnings = capital[3]

            # расчет суммарного капитала банка на указанную дату
            bank_total_capital = reserve_fund + equity_capital + accumulated_earnings
            return bank_total_capital

        search_date -= timedelta(days=1)  # Уменьшаем дату поиска на 1 день, если данные не найдены для текущей даты

    return None


if __name__ == '__main__':
    # Использование переменных окружения
    load_dotenv()

    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    delta_days = int(os.getenv("DELTA_DAYS"))

    # Использование парсера
    parser = argparse.ArgumentParser()
    parser.add_argument('date',
                        nargs='?',
                        help='Дата, на которую будет сумм параметров. Формат 2024-11-01',
                        default=datetime.now().strftime('%Y-%m-%d'))

    parser.add_argument('start_date_str',
                        type=str,
                        nargs='?',
                        help='Дата, с которой начинается загрузка. Пример "2024-11-01"',
                        default=None)
    parser.add_argument('end_date_str',
                        type=str,
                        nargs='?',
                        help='Дата, которой завершается загрузка. Пример "2024-11-07"',
                        default=None)
    parser.add_argument('step',
                        type=int,
                        nargs='?',
                        help='Шаг загрузки дат. Если 1 то последовательно один за другим. Пример 1',
                        default=1)

    args = parser.parse_args()

    # Подключение к базе данных
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host
    )
    cur = conn.cursor()

    cur.execute("SELECT date FROM dwh.common_data")
    dates = [row[0] for row in cur.fetchall()]

    # расчет показателей при указании диапазона дат при запуске ETL (функции)
    if args.start_date_str and args.end_date_str and args.step:
        start_date = datetime.strptime(args.start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(args.end_date_str, '%Y-%m-%d')

        current_date = start_date
        while current_date <= end_date:
            create_common_data(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=args.step)
        print("Данные на все даты, указанные в заданном периоде, рассчитаны и загружены")
    else:
        create_common_data(args.date)
        print(f"Данные на {args.date} рассчитаны и загружены.")

    # Сохраняем изменения
    conn.commit()

    # Закрываем соединение
    cur.close()
    conn.close()
