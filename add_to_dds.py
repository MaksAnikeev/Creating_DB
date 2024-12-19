import argparse

import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os


def add_companies():
    # Загрузка данных из staging.clients
    cur.execute('SELECT * FROM staging.companies')
    rows = cur.fetchall()

    cur.execute('SELECT MAX(timestamp_column) FROM dds.deposits_companies')
    last_timestamp = cur.fetchone()[0]

    for row in rows:
        name = row[1]
        phone_number = row[2]
        address = row[3]
        registration_date = row[4]
        email = row[5]
        inn = row[6]
        deposit_amount = row[7]
        opening_date = row[8]
        closing_date = row[9]
        interest_rate = row[10]
        timestamp_column = row[12]

        # Проверка условия по времени и добавление в dds.deposits_clients
        if not last_timestamp or timestamp_column > last_timestamp:
            # Добавление данных в dds.clients
            cur.execute('INSERT INTO dds.companies (name,phone_number,address,registration_date,email,inn,'
                        ' timestamp_column) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING company_id',
                        (name, phone_number, address, registration_date, email, inn, timestamp_column ))
            new_company_id = cur.fetchone()[0]

            # Добавление в dds.deposits_clients
            cur.execute('INSERT INTO dds.deposits_companies (deposit_amount, opening_date, closing_date, interest_rate,'
                        ' company_id, timestamp_column)'
                        ' VALUES (%s, %s, %s, %s, %s, %s)', (deposit_amount, opening_date, closing_date,
                                                             interest_rate, new_company_id, timestamp_column))


def add_clients():
    # Загрузка данных из staging.companies
    cur.execute('SELECT * FROM staging.clients')
    rows = cur.fetchall()

    cur.execute('SELECT MAX(timestamp_column) FROM dds.deposits_clients')
    last_timestamp = cur.fetchone()[0]

    for row in rows:
        first_name = row[1]
        last_name = row[2]
        address = row[3]
        phone_number = row[4]
        registration_date = row[5]
        email = row[6]
        deposit_amount = row[7]
        opening_date = row[8]
        closing_date = row[9]
        interest_rate = row[10]
        timestamp_column = row[12]

        # Проверка условия по времени и добавление в dds.deposits_clients
        if not last_timestamp or timestamp_column > last_timestamp:
            # Добавление данных в dds.clients
            cur.execute('INSERT INTO dds.clients (first_name, last_name, address, phone_number, registration_date,'
                        ' email, timestamp_column)'
                        ' VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING client_id',
                        (first_name, last_name, address, phone_number, registration_date, email, timestamp_column))
            new_client_id = cur.fetchone()[0]

            # Добавление в dds.deposits_clients
            cur.execute('INSERT INTO dds.deposits_clients (deposit_amount, opening_date, closing_date, interest_rate, client_id, timestamp_column)'
                        ' VALUES (%s, %s, %s, %s, %s, %s)', (deposit_amount, opening_date, closing_date,
                                                             interest_rate, new_client_id, timestamp_column))


def add_bank():
    # Загрузка данных из staging.bank
    cur.execute('SELECT * FROM staging.bank')
    rows = cur.fetchall()

    cur.execute('SELECT MAX(timestamp_column) FROM dds.bank')
    last_timestamp = cur.fetchone()[0]

    for row in rows:
        name = row[1]
        address = row[2]
        license_number = row[3]
        timestamp_column = row[4]

        # Проверка условия по времени и добавление в dds.deposits_clients
        if not last_timestamp or timestamp_column > last_timestamp:
            # Добавление данных в dds.bank
            cur.execute(
                'INSERT INTO dds.bank (name, address, license_number, timestamp_column)'
                ' VALUES (%s, %s, %s, %s)',
                (name, address, license_number, timestamp_column))


def add_capital(history=False, date=None):
    # выбор объекта с последним timestamp_column на текущую дату staging.capital
    if history:
        # Загрузка данных из staging.capital
        cur.execute('''
            SELECT * 
            FROM staging.capital c
            WHERE timestamp_column = (
                SELECT MAX(timestamp_column)
                FROM staging.capital
                WHERE CAST(timestamp_column AS date) = CAST(c.timestamp_column AS date)
            )
            ''')
        latest_capital = cur.fetchall()

    else:
        if date:
            current_date_check = date
        else:
            current_date_check = datetime.now().date()
        current_datetime = datetime.now().replace(microsecond=0)
        current_date = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

        # Загрузка последнего объекта на текущую дату из staging.capital
        cur.execute(
            "SELECT * FROM staging.capital WHERE CAST(timestamp_column AS date) = %s ORDER BY timestamp_column DESC LIMIT 1",
            (current_date_check,))

        # Получаем единственный объект
        latest_capital = cur.fetchone()

        if latest_capital:
            latest_capital = [latest_capital]

    if latest_capital:
        # Получение последнего объекта из dds.capital для проверки обновления
        cur.execute('SELECT MAX(timestamp_column) FROM dds.capital')
        last_timestamp = cur.fetchone()[0]

        for row in latest_capital:
            reserve_fund = row[1]
            equity_capital = row[2]
            accumulated_earnings = row[3]
            timestamp_column = row[5]

            if date:
                # Получение последнего ID банка из dds.bank
                cur.execute("SELECT id FROM dds.bank ORDER BY id DESC LIMIT 1")
                bank_id = cur.fetchone()[0]

                # Добавление данных в dds.bank
                current_date = date

                cur.execute(
                    'INSERT INTO dds.capital (reserve_fund, equity_capital, accumulated_earnings, timestamp_column,'
                    ' bank_id)'
                    ' VALUES (%s, %s, %s, %s, %s)',
                    (reserve_fund, equity_capital, accumulated_earnings, current_date, bank_id))

            # Проверка условия по времени и добавление в dds.deposits_clients
            if not last_timestamp or timestamp_column > last_timestamp:
                # Получение последнего ID банка из dds.bank
                cur.execute("SELECT id FROM dds.bank ORDER BY id DESC LIMIT 1")
                bank_id = cur.fetchone()[0]

                # Добавление данных в dds.bank
                if history:
                    current_date = timestamp_column

                cur.execute(
                    'INSERT INTO dds.capital (reserve_fund, equity_capital, accumulated_earnings, timestamp_column,'
                    ' bank_id)'
                    ' VALUES (%s, %s, %s, %s, %s)',
                    (reserve_fund, equity_capital, accumulated_earnings, current_date, bank_id))


def add_assets(history=False, date=None):
    # выбор объекта с последним timestamp_column на текущую дату staging.general_assets
    if history:
        # Загрузка данных из staging.general_assets
        cur.execute('''
                    SELECT * 
                    FROM staging.general_assets c
                    WHERE timestamp_column = (
                        SELECT MAX(timestamp_column)
                        FROM staging.general_assets
                        WHERE CAST(timestamp_column AS date) = CAST(c.timestamp_column AS date)
                    )
                    ''')
        latest_assets = cur.fetchall()

    else:
        if date:
            current_date_check = date
        else:
            current_date_check = datetime.now().date()
        current_datetime = datetime.now().replace(microsecond=0)
        current_date = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

        # Загрузка последнего объекта на текущую дату из staging.capital
        cur.execute(
            "SELECT * FROM staging.general_assets WHERE CAST(timestamp_column AS date) = %s ORDER BY timestamp_column DESC LIMIT 1",
            (current_date_check,))

        # Получаем единственный объект
        latest_assets = cur.fetchone()

        if latest_assets:
            latest_assets = [latest_assets]

    if latest_assets:
        # Получение последнего объекта из dds.capital для проверки обновления
        cur.execute('SELECT MAX(timestamp_column) FROM dds.general_assets')
        last_timestamp = cur.fetchone()[0]

        for row in latest_assets:
            securities = row[1]
            real_estate = row[2]
            financial_reports = row[3]
            credit_facilities = row[4]
            machinery = row[5]
            debts = row[6]
            equipment = row[7]
            timestamp_column = row[9]

            if date:
                # Получение последнего ID банка из dds.bank
                cur.execute("SELECT id FROM dds.bank ORDER BY id DESC LIMIT 1")
                bank_id = cur.fetchone()[0]

                # Добавление данных в dds.bank
                current_date = date

                cur.execute(
                    'INSERT INTO dds.general_assets (securities, real_estate, financial_reports, credit_facilities,'
                    ' machinery, debts, equipment, timestamp_column, bank_id)'
                    ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (securities, real_estate, financial_reports, credit_facilities, machinery, debts, equipment,
                     current_date, bank_id))

            # Проверка условия по времени и добавление в dds.deposits_clients
            if not last_timestamp or timestamp_column > last_timestamp:
                # Получение последнего ID банка из dds.bank
                cur.execute("SELECT id FROM dds.bank ORDER BY id DESC LIMIT 1")
                bank_id = cur.fetchone()[0]

                # Добавление данных в dds.bank
                if history:
                    current_date = timestamp_column

                cur.execute(
                    'INSERT INTO dds.general_assets (securities, real_estate, financial_reports, credit_facilities,'
                    ' machinery, debts, equipment, timestamp_column, bank_id)'
                    ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    (securities, real_estate, financial_reports, credit_facilities, machinery, debts, equipment,
                     current_date, bank_id))


def add_control_liabilities(history=False, date=None):
    # выбор объекта с последним timestamp_column на текущую дату staging.control_liabilities
    if history:
        # Загрузка данных из staging.general_assets
        cur.execute('''
                    SELECT * 
                    FROM staging.control_liabilities c
                    WHERE timestamp_column = (
                        SELECT MAX(timestamp_column)
                        FROM staging.control_liabilities
                        WHERE CAST(timestamp_column AS date) = CAST(c.timestamp_column AS date)
                    )
                    ''')
        latest_liabilities = cur.fetchall()

    else:
        if date:
            current_date_check = date
        else:
            current_date_check = datetime.now().date()
        current_datetime = datetime.now().replace(microsecond=0)
        current_date = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

        # Загрузка последнего объекта на текущую дату из staging.capital
        cur.execute(
            "SELECT * FROM staging.control_liabilities WHERE CAST(timestamp_column AS date) = %s ORDER BY timestamp_column DESC LIMIT 1",
            (current_date_check,))

        # Получаем единственный объект
        latest_liabilities = cur.fetchone()
        if latest_liabilities:
            latest_liabilities = [latest_liabilities]


    if latest_liabilities:
        # Получение последнего объекта из dds.capital для проверки обновления
        cur.execute('SELECT MAX(timestamp_column) FROM dds.control_liabilities')
        last_timestamp = cur.fetchone()[0]

        for row in latest_liabilities:
            financial_instruments_debts = row[1]
            securities_obligations = row[2]
            reporting_data = row[3]
            invoices_to_pay = row[4]
            funds_in_accounts = row[5]
            timestamp_column = row[7]

            if date:
                # Получение последнего ID банка из dds.bank
                cur.execute("SELECT id FROM dds.bank ORDER BY id DESC LIMIT 1")
                bank_id = cur.fetchone()[0]

                # Добавление данных в dds.bank
                current_date = date

                cur.execute(
                    'INSERT INTO dds.control_liabilities (financial_instruments_debts, securities_obligations,'
                    ' reporting_data, invoices_to_pay, funds_in_accounts, timestamp_column, bank_id)'
                    ' VALUES (%s, %s, %s, %s, %s, %s, %s)',
                    (financial_instruments_debts, securities_obligations, reporting_data, invoices_to_pay,
                     funds_in_accounts, current_date, bank_id))

            # Проверка условия по времени и добавление в dds.deposits_clients
            if not last_timestamp or timestamp_column > last_timestamp:
                # Получение последнего ID банка из dds.bank
                cur.execute("SELECT id FROM dds.bank ORDER BY id DESC LIMIT 1")
                bank_id = cur.fetchone()[0]

                # Добавление данных в dds.bank
                if history:
                    current_date = timestamp_column

                cur.execute(
                    'INSERT INTO dds.control_liabilities (financial_instruments_debts, securities_obligations,'
                    ' reporting_data, invoices_to_pay, funds_in_accounts, timestamp_column, bank_id)'
                    ' VALUES (%s, %s, %s, %s, %s, %s, %s)',
                    (financial_instruments_debts, securities_obligations, reporting_data, invoices_to_pay,
                     funds_in_accounts, current_date, bank_id))


if __name__ == '__main__':
    # Использование переменных окружения
    load_dotenv()

    db_name = os.getenv("DB_NAME")
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")

    # Использование парсера
    parser = argparse.ArgumentParser()
    parser.add_argument('history',
                        nargs='?',
                        help='Загрузка не только текущей даты, но и всех предыдущих. '
                             'history_capital - данные о капитале банка'
                             'history_liabilities - данные о пассивах банка.'
                             'history_assets - данные о активах банка.'
                             'all - загрузка всех данных на текущий день.'
                             'history_all - загрузка всех данных, с последней даты загрузки по текущий день.',
                        default='all')

    parser.add_argument('date',
                        nargs='?',
                        help='Загрузка данных только на указанную дату',
                        default=None)

    args = parser.parse_args()

    # Подключение к базе данных
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_pass,
        host=db_host
    )
    cur = conn.cursor()

    if args.history == 'history_capital':
        add_capital(history=True)
        print('Загрузка данных по капиталу с момента последней загрузки, до текущего момента - завершена.')
    elif args.history == 'history_liabilities':
        add_control_liabilities(history=True)
        print('Загрузка данных по активам с момента последней загрузки, до текущего момента - завершена.')
    elif args.history == 'history_assets':
        add_assets(history=True)
        print('Загрузка данных по пассивам с момента последней загрузки, до текущего момента - завершена.')
    elif args.history == 'all':
        add_clients()
        add_companies()
        add_bank()
        add_capital()
        add_assets()
        add_control_liabilities()
        print('Загрузка всех данных по текущей дате - завершена.')
    elif args.history == 'history_all':
        add_clients()
        add_companies()
        add_bank()
        add_capital(history=True)
        add_assets(history=True)
        add_control_liabilities(history=True)
        print('Загрузка всех с момента последней загрузки, до текущего момента - завершена.')
    elif args.history == 'None' and args.date:
        add_clients()
        add_companies()
        add_bank()
        add_control_liabilities(history=False, date=args.date)
        add_capital(history=False, date=args.date)
        add_assets(history=False, date=args.date)

    else:
        print("""Вы ввели некорректный параметр. Введите:
                 history_capital - данные о капитале банка
                 history_liabilities - данные о пассивах банка.
                 history_assets - данные о активах банка.
                 all - загрузка всех данных на текущий день.
                 history_all - загрузка всех данных, с последней даты загрузки по текущий день.""")


    # Сохраняем изменения
    conn.commit()

    # Закрываем соединение
    cur.close()
    conn.close()
