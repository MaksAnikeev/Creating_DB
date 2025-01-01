import argparse
import csv
import os
from datetime import datetime
from typing import Any, Callable, List, Tuple

import psycopg2
from dotenv import load_dotenv


def reading_file(path_file: str, loading_function: Callable[[csv.reader, str], None]) -> None:
    """Функция считывает данные с файла в список списков строк и передает данный список
     в указанную функцию для загрузки информации в базу данных."""
    file_name = os.path.basename(path_file)

    # Открываем CSV файл для чтения
    with open(path_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Пропускаем заголовок
        loading_function(reader, file_name)


def loading_clients(clients_info: List[Tuple[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], file_name: str) \
        -> None:
    """Функция загружает информацию по клиентам из списка списков в БД"""
    for client in clients_info:
        first_name, last_name, address, phone_number, registration_date, email, deposit_amount, opening_date, \
        closing_date, interest_rate = client

        # Проверяем уникальность записи перед загрузкой
        cur.execute("""
            SELECT id FROM staging.clients
            WHERE first_name = %s
            AND last_name = %s
            AND address = %s
            AND phone_number = %s
            AND registration_date = %s
            AND email = %s
            AND deposit_amount = %s
            AND opening_date = %s
            AND closing_date = %s
            AND interest_rate = %s""", (first_name, last_name, address, phone_number, registration_date, email,
                                        deposit_amount, opening_date, closing_date, interest_rate))
        existing_record = cur.fetchone()

        if existing_record is None:
            timestamp_column = datetime.now()

            # Вставляем новую запись
            cur.execute("""
                INSERT INTO staging.clients
                (first_name, last_name, address, phone_number, registration_date, email,
                deposit_amount, opening_date, closing_date, interest_rate, file_name, timestamp_column)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (first_name, last_name, address, phone_number, registration_date, email, deposit_amount,
                         opening_date, closing_date, interest_rate, file_name, timestamp_column))


def loading_companies(companies_info: List[Tuple[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], file_name: str) \
        -> None:
    """Функция загружает информацию по компаниям из списка списков в БД"""
    for company in companies_info:
        name, phone_number, address, registration_date, email, inn, deposit_amount, opening_date, closing_date, \
        interest_rate = company

        # Проверяем уникальность записи перед загрузкой
        cur.execute("""
            SELECT id FROM staging.companies
            WHERE name = %s
            AND phone_number = %s
            AND address = %s
            AND registration_date = %s
            AND email = %s
            AND inn = %s
            AND deposit_amount = %s
            AND opening_date = %s
            AND closing_date = %s
            AND interest_rate = %s""", (name, phone_number, address, registration_date, email, inn, deposit_amount,
                                        opening_date, closing_date, interest_rate))
        existing_record = cur.fetchone()

        if existing_record is None:
            timestamp_column = datetime.now()

            # Вставляем новую запись
            cur.execute("""
                INSERT INTO staging.companies
                (name, phone_number, address, registration_date, email, inn, deposit_amount, opening_date,
                closing_date, interest_rate, file_name, timestamp_column)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (name, phone_number, address, registration_date, email, inn, deposit_amount,
                         opening_date, closing_date, interest_rate, file_name, timestamp_column))


def loading_bank(bank_info: List[Tuple[str, str, str]], file_name: str) -> None:
    """Функция загружает банковскую информацию в БД"""
    for bank_data in bank_info:
        name, address, license_number = bank_data

        # Проверяем уникальность записи перед загрузкой
        cur.execute("""
            SELECT id FROM staging.bank
            WHERE name = %s
            AND address = %s
            AND license_number = %s""", (name, address, license_number))

        existing_record = cur.fetchone()

        if existing_record is None:
            timestamp_column = datetime.now()

            # Вставляем новую запись
            cur.execute("""
                INSERT INTO staging.bank
                (name, address, license_number,timestamp_column)
                VALUES (%s, %s, %s, %s)""",
                        (name, address, license_number, timestamp_column))


def loading_capital(capital_info: List, file_name: str) -> None:
    """Функция загружает информацию по капиталу банка на разные даты из списка списков в БД"""
    for capital_data in capital_info:
        if len(capital_data) == 3:
            reserve_fund, equity_capital, accumulated_earnings = capital_data
            timestamp_column = datetime.now()
            # Проверяем уникальность записи перед загрузкой
            cur.execute("""
                        SELECT id FROM staging.capital
                        WHERE reserve_fund = %s
                        AND equity_capital = %s
                        AND accumulated_earnings = %s
                        """,
                        (reserve_fund, equity_capital, accumulated_earnings))

            existing_record = cur.fetchone()
        elif len(capital_data) == 4:
            reserve_fund, equity_capital, accumulated_earnings, timestamp_column = capital_data
            # Проверяем уникальность записи перед загрузкой
            cur.execute("""
                        SELECT id FROM staging.capital
                        WHERE reserve_fund = %s
                        AND equity_capital = %s
                        AND accumulated_earnings = %s
                        AND timestamp_column = %s""",
                        (reserve_fund, equity_capital, accumulated_earnings, timestamp_column))

            existing_record = cur.fetchone()
        else:
            print("Incorrect number of values in capital_data")
            continue

        if existing_record is None:
            # Вставляем новую запись
            cur.execute("""
                INSERT INTO staging.capital
                (reserve_fund, equity_capital, accumulated_earnings,file_name, timestamp_column)
                VALUES (%s, %s, %s, %s, %s)""",
                        (reserve_fund, equity_capital, accumulated_earnings, file_name, timestamp_column))


def loading_liabilities(liabilities_info: List[Tuple[str, str, str, str, str]],
                        file_name: str) -> None:
    """Функция загружает информацию по пассивам банка на разные даты из списка списков в БД"""
    for liabilities_data in liabilities_info:
        if len(liabilities_data) == 5:
            financial_instruments_debts, securities_obligations, reporting_data, invoices_to_pay, \
            funds_in_accounts = liabilities_data
            timestamp_column = datetime.now()
            cur.execute("""
                        SELECT id FROM staging.control_liabilities
                        WHERE financial_instruments_debts = %s
                        AND securities_obligations = %s
                        AND reporting_data = %s
                        AND invoices_to_pay = %s
                        AND funds_in_accounts = %s
                        """,
                        (financial_instruments_debts, securities_obligations, reporting_data,
                         invoices_to_pay, funds_in_accounts))

            existing_record = cur.fetchone()

        elif len(liabilities_data) == 6:
            financial_instruments_debts, securities_obligations, reporting_data, invoices_to_pay, \
            funds_in_accounts, timestamp_column = liabilities_data
            cur.execute("""
                        SELECT id FROM staging.control_liabilities
                        WHERE financial_instruments_debts = %s
                        AND securities_obligations = %s
                        AND reporting_data = %s
                        AND invoices_to_pay = %s
                        AND funds_in_accounts = %s
                        AND timestamp_column = %s""",
                        (financial_instruments_debts, securities_obligations, reporting_data,
                         invoices_to_pay, funds_in_accounts, timestamp_column))

            existing_record = cur.fetchone()
        else:
            print("Incorrect number of values in capital_data")
            continue

        if existing_record is None:
            # Вставляем новую запись
            cur.execute("""
                INSERT INTO staging.control_liabilities
                (financial_instruments_debts, securities_obligations, reporting_data, invoices_to_pay,
                         funds_in_accounts, file_name, timestamp_column)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (financial_instruments_debts, securities_obligations, reporting_data, invoices_to_pay,
                         funds_in_accounts, file_name, timestamp_column))


def loading_assets(assets_info: List[Tuple[str, str, str, str, str, str, str]],
                   file_name: str) -> None:
    """Функция загружает информацию по активам банка на разные даты из списка списков в БД"""
    for assets_data in assets_info:
        if len(assets_data) == 7:
            securities, real_estate, financial_reports, credit_facilities, machinery, debts, equipment = assets_data
            timestamp_column = datetime.now()
            # Проверяем уникальность записи перед загрузкой
            cur.execute("""
                        SELECT id FROM staging.general_assets
                        WHERE securities = %s
                        AND real_estate = %s
                        AND financial_reports = %s
                        AND credit_facilities = %s
                        AND machinery = %s
                        AND debts = %s
                        AND equipment = %s
                        """, (securities, real_estate, financial_reports, credit_facilities,
                              machinery, debts, equipment))

            existing_record = cur.fetchone()
        elif len(assets_data) == 8:
            securities, real_estate, financial_reports, credit_facilities, machinery, debts, equipment, \
            timestamp_column = assets_data
            # Проверяем уникальность записи перед загрузкой
            cur.execute("""
                        SELECT id FROM staging.general_assets
                        WHERE securities = %s
                        AND real_estate = %s
                        AND financial_reports = %s
                        AND credit_facilities = %s
                        AND machinery = %s
                        AND debts = %s
                        AND equipment = %s
                        AND timestamp_column = %s""", (securities, real_estate, financial_reports, credit_facilities,
                                                       machinery, debts, equipment, timestamp_column))

            existing_record = cur.fetchone()
        else:
            print("Incorrect number of values in capital_data")
            continue

        if existing_record is None:
            # Вставляем новую запись
            cur.execute("""
                INSERT INTO staging.general_assets
                (securities, real_estate, financial_reports, credit_facilities, machinery, debts,
                 equipment, file_name, timestamp_column)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (securities, real_estate, financial_reports, credit_facilities, machinery,
                         debts, equipment, file_name, timestamp_column))


if __name__ == '__main__':
    # Использование переменных окружения
    load_dotenv()

    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    path_clients = os.getenv("PATH_CLIENTS")
    path_companies = os.getenv("PATH_COMPANIES")
    path_bank = os.getenv("PATH_BANK")
    path_capital = os.getenv("PATH_CAPITAL")
    path_liabilities = os.getenv("PATH_LIABILITIES")
    path_assets = os.getenv("PATH_ASSETS")

    # Использование парсера
    parser = argparse.ArgumentParser()
    parser.add_argument('loading',
                        nargs='?',
                        help='Название загружаемых данных. '
                             'сlients - загрузка данных клиентов.'
                             'companies - загрузка данных компаний.'
                             'bank - загрузка информации о банке.'
                             'capital - загрузка данных о капитале банка.'
                             'liabilities - загрузка данных о пасивах банка.'
                             'assets - загрузка данных о активах банка.'
                             'all - загрузка всех данных в БД.',
                        default='all')

    args = parser.parse_args()

    # Подключение к базе данных
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host
    )
    cur = conn.cursor()

    if args.loading == 'clients':
        reading_file(path_file=path_clients, loading_function=loading_clients)
        print('Загрузка данных клиентов в БД успешно завершена')

    elif args.loading == 'companies':
        reading_file(path_file=path_companies, loading_function=loading_companies)
        print('Загрузка данных компаний в БД успешно завершена')

    elif args.loading == 'bank':
        reading_file(path_file=path_bank, loading_function=loading_bank)
        print('Загрузка данных о банке в БД успешно завершена')

    elif args.loading == 'capital':
        reading_file(path_file=path_capital, loading_function=loading_capital)
        print('Загрузка данных о капитале банка в БД успешно завершена')

    elif args.loading == 'liabilities':
        reading_file(path_file=path_liabilities, loading_function=loading_liabilities)
        print('Загрузка данных о пассивах банка в БД успешно завершена')

    elif args.loading == 'assets':
        reading_file(path_file=path_assets, loading_function=loading_assets)
        print('Загрузка данных о активах банка в БД успешно завершена')

    elif args.loading == 'all':
        reading_file(path_file=path_clients, loading_function=loading_clients)
        reading_file(path_file=path_companies, loading_function=loading_companies)
        reading_file(path_file=path_bank, loading_function=loading_bank)
        reading_file(path_file=path_capital, loading_function=loading_capital)
        reading_file(path_file=path_liabilities, loading_function=loading_liabilities)
        reading_file(path_file=path_assets, loading_function=loading_assets)
        print('Загрузка всех данных в БД успешно завершена')

    else:
        print("""Вы ввели некорректный параметр. Введите:
              сlients - загрузка данных клиентов.
              companies - загрузка данных компаний.
              bank - загрузка информации о банке.'
              capital - загрузка данных о капитале банка.
              liabilities - загрузка данных о пасивах банка.
              assets - загрузка данных о активах банка.
              all - загрузка всех данных в БД.""")

    # Сохраняем изменения
    conn.commit()

    # Закрываем соединение
    cur.close()
    conn.close()
