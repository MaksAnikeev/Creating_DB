import argparse
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Optional

import psycopg2
from dotenv import load_dotenv


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def add_companies() -> None:
    cur.execute('SELECT * FROM dds.companies')
    rows = cur.fetchall()

    cur.execute('SELECT MAX(timestamp_column) FROM dwh.companies')
    last_timestamp = cur.fetchone()[0]

    for row in rows:
        company_id = row[0]
        name = row[1]
        phone_number = row[2]
        address = row[3]
        registration_date = row[4]
        email = row[5]
        inn = row[6]
        timestamp_column = row[7]

        # Проверка условия по времени и добавление в dds.deposits_clients
        if not last_timestamp or timestamp_column > last_timestamp:
            # Добавление данных в dwh.clients
            cur.execute('INSERT INTO dwh.companies (name, phone_number,address,registration_date,email,inn,'
                        ' timestamp_column) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING company_id',
                        (name, phone_number, address, registration_date, email, inn, timestamp_column))
            new_company_id = cur.fetchone()[0]

            cur.execute("""
            SELECT * 
            FROM dds.deposits_companies
            WHERE company_id = %s;
            """, (company_id,))

            company_deposit = [cur.fetchone()]

            for row in company_deposit:
                deposit_amount = row[1]
                opening_date = row[2]
                closing_date = row[3]
                interest_rate = row[4]
                timestamp_column = row[6]

            # Добавление в dwh.deposits_companies
            cur.execute('INSERT INTO dwh.deposits_companies (deposit_amount, opening_date, closing_date, interest_rate,'
                        ' company_id, timestamp_column)'
                        ' VALUES (%s, %s, %s, %s, %s, %s)', (deposit_amount, opening_date, closing_date,
                                                             interest_rate, new_company_id, timestamp_column))


def add_clients() -> None:
    cur.execute('SELECT * FROM dds.clients')
    rows = cur.fetchall()

    cur.execute('SELECT MAX(timestamp_column) FROM dwh.clients')
    last_timestamp = cur.fetchone()[0]

    for row in rows:
        client_id = row[0]
        first_name = row[1]
        last_name = row[2]
        address = row[3]
        phone_number = row[4]
        registration_date = row[5]
        email = row[6]
        timestamp_column = row[7]

        # Проверка условия по времени и добавление в dds.deposits_clients
        if not last_timestamp or timestamp_column > last_timestamp:
            cur.execute("""
                        SELECT * 
                        FROM dds.deposits_clients
                        WHERE client_id = %s;
                        """, (client_id,))

            client_deposit = [cur.fetchone()]

            # Добавление данных в dds.clients
            cur.execute('INSERT INTO dwh.clients (first_name, last_name, address, phone_number, registration_date,'
                        ' email, timestamp_column)'
                        ' VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING client_id',
                        (first_name, last_name, address, phone_number, registration_date, email, timestamp_column))
            new_client_id = cur.fetchone()[0]

            for row in client_deposit:
                deposit_amount = row[1]
                opening_date = row[2]
                closing_date = row[3]
                interest_rate = row[4]
                timestamp_column = row[6]

            # Добавление в dwh.deposits_clients
            cur.execute('INSERT INTO dwh.deposits_clients (deposit_amount, opening_date, closing_date, interest_rate,'
                        ' client_id, timestamp_column)'
                        ' VALUES (%s, %s, %s, %s, %s, %s)', (deposit_amount, opening_date, closing_date,
                                                             interest_rate, new_client_id, timestamp_column))


def add_bank() -> None:
    # Выборка данных из таблицы dds.bank, которых нет в таблице dwh.bank
    cur.execute(f"""
        SELECT *
        FROM dds.bank
        WHERE NOT EXISTS (
            SELECT 1
            FROM dwh.bank
            WHERE 
            dds.bank.name = dwh.bank.name AND
            dds.bank.address = dwh.bank.address AND
            dds.bank.license_number = dwh.bank.license_number
        )
    """)

    rows = cur.fetchall()

    timestamp_column = datetime.now()

    # Вставка данных в таблицу dwh.capital
    for row in rows:
        bank_data = (*row[1:-1], timestamp_column)
        cur.execute("""
            INSERT INTO dwh.bank (name, address, license_number, timestamp_column)
            VALUES (%s, %s, %s, %s)
        """, bank_data)


def add_capital(target_date: Optional[str] = None) -> None:
    # Формирование условия для выборки по дате, если указана
    date_condition = f"timestamp_column::date = '{target_date}'" if target_date else "TRUE"

    # Выборка данных из таблицы dds.capital, которых нет в таблице dwh.capital
    cur.execute(f"""
        SELECT *
        FROM dds.capital
        WHERE {date_condition}
        AND NOT EXISTS (
            SELECT 1
            FROM dwh.capital
            WHERE 
            dds.capital.reserve_fund = dwh.capital.reserve_fund AND
            dds.capital.equity_capital = dwh.capital.equity_capital AND
            dds.capital.accumulated_earnings = dwh.capital.accumulated_earnings AND
            dds.capital.timestamp_column = dwh.capital.timestamp_column
        )
    """)

    rows = cur.fetchall()

    cur.execute("SELECT id FROM dwh.bank ORDER BY id DESC LIMIT 1")
    bank_id = cur.fetchone()[0]

    # Вставка данных в таблицу dwh.capital
    for row in rows:
        bank_data = (*row[1:-1], bank_id)
        cur.execute("""
            INSERT INTO dwh.capital (reserve_fund, equity_capital, accumulated_earnings, timestamp_column, bank_id)
            VALUES (%s, %s, %s, %s, %s)
        """, bank_data)


def add_assets(target_date: Optional[str] = None) -> None:
    # Формирование условия для выборки по дате, если указана
    date_condition = f"timestamp_column::date = '{target_date}'" if target_date else "TRUE"

    # Выборка данных из таблицы dds.general_assets, которых нет в таблице dwh.general_assets
    cur.execute(f"""
        SELECT *
        FROM dds.general_assets
        WHERE {date_condition}
        AND NOT EXISTS (
            SELECT 1
            FROM dwh.general_assets
            WHERE 
            dds.general_assets.securities = dwh.general_assets.securities AND
            dds.general_assets.real_estate = dwh.general_assets.real_estate AND
            dds.general_assets.financial_reports = dwh.general_assets.financial_reports AND
            dds.general_assets.credit_facilities = dwh.general_assets.credit_facilities AND
            dds.general_assets.machinery = dwh.general_assets.machinery AND
            dds.general_assets.debts = dwh.general_assets.debts AND
            dds.general_assets.equipment = dwh.general_assets.equipment AND
            dds.general_assets.timestamp_column = dwh.general_assets.timestamp_column
        )
    """)

    rows = cur.fetchall()

    cur.execute("SELECT id FROM dwh.bank ORDER BY id DESC LIMIT 1")
    bank_id = cur.fetchone()[0]

    # Вставка данных в таблицу dwh.capital
    for row in rows:
        bank_data = (*row[1:-1], bank_id)
        cur.execute("""
            INSERT INTO dwh.general_assets (securities, real_estate, financial_reports, credit_facilities, 
                     machinery, debts, equipment, timestamp_column, bank_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, bank_data)


def add_control_liabilities(target_date: Optional[str] = None) -> None:
    # Формирование условия для выборки по дате, если указана
    date_condition = f"timestamp_column::date = '{target_date}'" if target_date else "TRUE"

    # Выборка данных из таблицы dds.control_liabilities, которых нет в таблице dwh.control_liabilities
    cur.execute(f"""
        SELECT *
        FROM dds.control_liabilities
        WHERE {date_condition}
        AND NOT EXISTS (
            SELECT 1
            FROM dwh.control_liabilities
            WHERE 
            dds.control_liabilities.financial_instruments_debts = dwh.control_liabilities.financial_instruments_debts AND
            dds.control_liabilities.securities_obligations = dwh.control_liabilities.securities_obligations AND
            dds.control_liabilities.reporting_data = dwh.control_liabilities.reporting_data AND
            dds.control_liabilities.invoices_to_pay = dwh.control_liabilities.invoices_to_pay AND
            dds.control_liabilities.funds_in_accounts = dwh.control_liabilities.funds_in_accounts AND
            dds.control_liabilities.timestamp_column = dwh.control_liabilities.timestamp_column        
        )
    """)

    rows = cur.fetchall()

    cur.execute("SELECT id FROM dwh.bank ORDER BY id DESC LIMIT 1")
    bank_id = cur.fetchone()[0]

    # Вставка данных в таблицу dwh.capital
    for row in rows:
        bank_data = (*row[1:-1], bank_id)
        cur.execute("""
            INSERT INTO dwh.control_liabilities (financial_instruments_debts, securities_obligations,
                     reporting_data, invoices_to_pay, funds_in_accounts, timestamp_column, bank_id)
                     VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, bank_data)


if __name__ == '__main__':
    # Запись логов в файл
    file_handler = RotatingFileHandler(os.path.join('logs', 'add_to_dwh.log'),
                                       maxBytes=2*1024*1024,
                                       backupCount=1)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)

    # Вывод логов в консоль
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    # Использование переменных окружения
    load_dotenv()

    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")

    # Использование парсера
    parser = argparse.ArgumentParser()
    parser.add_argument('target_date',
                        nargs='?',
                        help='Загрузка на указанную дату. '
                             'target_capital - данные о капитале банка'
                             'target_liabilities - данные о пассивах банка.'
                             'target_assets - данные о активах банка.'
                             'all - загрузка всех данных.'
                             'current_all - загрузка всех данных на текущий день.',
                        default='current_all')

    parser.add_argument('date',
                        nargs='?',
                        help='Дата, на которую будет загрузка.',
                        default=datetime.now())

    args = parser.parse_args()

    # Подключение к базе данных
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host
    )
    cur = conn.cursor()

    try:
        if args.target_date == 'target_capital':
            add_capital(target_date=args.date)
            logger.info("Данные о капитале банка на все ранее не загруженные даты - загружены. "
                        "Если вы ввели конкретную дату, то данные на эту дату загружены.")
        if args.target_date == 'target_liabilities':
            add_control_liabilities(target_date=args.date)
            logger.info("Данные о пассивах банка на все ранее не загруженные даты - загружены."
                        "Если вы ввели конкретную дату, то данные на эту дату загружены.")
        if args.target_date == 'target_assets':
            add_assets(target_date=args.date)
            logger.info("Данные об активах банка на все ранее не загруженные даты - загружены. "
                        "Если вы ввели конкретную дату, то данные на эту дату загружены.")

        if args.target_date == 'all':
            add_clients()
            add_companies()
            add_bank()
            add_capital()
            add_assets()
            add_control_liabilities()
            logger.info("Все данные на все ранее не загруженные даты - загружены."
                        "Если вы ввели конкретную дату, то данные на эту дату загружены.")

        if args.target_date == 'current_all':
            add_clients()
            add_companies()
            add_bank()
            add_capital(target_date=args.date)
            add_assets(target_date=args.date)
            add_control_liabilities(target_date=args.date)
            logger.info("Все данные загружены на текущую дату.")

    except Exception as e:
        logger.exception("Произошла ошибка во время выполнения файла: %s", e)

    # Сохраняем изменения
    conn.commit()

    # Закрываем соединение
    cur.close()
    conn.close()
