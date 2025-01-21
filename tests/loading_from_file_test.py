from datetime import datetime
from typing import Any, List, Tuple
from unittest.mock import ANY, call

import pytest
from loading_from_file import (loading_clients, loading_liabilities,
                               reading_file)

from conftest import cur_mock, path_capital, path_clients


def mock_loading_clients(clients_info: List[Tuple[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]], file_name: str,
                         cur):
    """Эмуляция поведения функции loading_clients - считывает данные с файла clients_test.csv
         и проверяет правильность присваивания переменных."""
    clients_params = []

    # clients_info получаем при отрабатывании функции reading_file
    for client in clients_info:
        first_name, last_name, address, phone_number, registration_date, email, deposit_amount, opening_date, \
        closing_date, interest_rate = client
        client_param = [first_name, last_name, interest_rate]
        clients_params.append(client_param)

    # проверка правильности считанных с файла данных
    expected_name = 'Алиса'
    expected_last_name = 'Смирнов'
    expected_interest_rate = '0.03'
    expected_file_name = 'clients_test.csv'
    assert clients_params[0][0] == expected_name
    assert clients_params[1][1] == expected_last_name
    assert clients_params[1][2] == expected_interest_rate
    assert file_name == expected_file_name


def mock_loading_capital(capital_info: List, file_name: str, cur):
    """Эмуляция поведения функции loading_capital - считывает данные с файла capital_test.csv
     и проверяет правильность присваивания переменных."""
    capital_params = []

    # clients_info получаем при отрабатывании функции reading_file
    for capital in capital_info:
        if len(capital) == 3:
            # если в объекте из файла нет timestamp_column, то объекту будет присвоено текущая дата и время
            reserve_fund, equity_capital, accumulated_earnings = capital
            expected_without_timestamp_column = timestamp_column = datetime.now()
        elif len(capital) == 4:
            # если в объекте из файла есть timestamp_column, то он будет использован в дальнейшем
            reserve_fund, equity_capital, accumulated_earnings, timestamp_column = capital
        else:
            logging_message = "Incorrect number of values in liabilities_data"
            continue
        capital_param = [reserve_fund, accumulated_earnings, timestamp_column]
        capital_params.append(capital_param)

    # проверка правильности считанных с файла данных
    expected_reserve_fund = 11111
    expected_accumulated_earnings = 1822.50
    expected_timestamp_column = '2024-06-02 00:20:00'
    expected_file_name = 'capital_test.csv'
    assert int(capital_params[0][0]) == expected_reserve_fund
    assert float(capital_params[1][1]) == expected_accumulated_earnings
    assert capital_params[1][2] == expected_timestamp_column
    assert capital_params[3][2] == expected_without_timestamp_column
    assert logging_message == "Incorrect number of values in liabilities_data"
    assert file_name == expected_file_name


def test_reading_file_clients():
    """Тест для проверки функции reading_file. Тестируем чтение и загрузку данных с файла clients_test.csv."""
    reading_file(path_file=path_clients, loading_function=mock_loading_clients, cur=cur_mock)


def test_reading_file_capital():
    """Тест для проверки функции reading_file. Тестируем чтение и загрузку данных с файла capital_test.csv."""
    reading_file(path_file=path_capital, loading_function=mock_loading_capital, cur=cur_mock)


def test_loading_clients_with_mocks(cur_mock):
    """Тест для проверки функции loading_clients. Имитирует подключение к БД
     и проверяет правильность отправки запросов."""
    # Устанавливаем возвращаемое значение для cur_mock.fetchone
    cur_mock.fetchone.return_value = None  # Возвращаем None для имитации отсутствия совпадающей записи

    # Подготовка данных для загрузки
    clients_info = [('John', 'Doe', '123 Main St', '555-1234', '2022-01-01', 'john.doe@example.com', 1000,
                     '2022-01-01', '2023-01-01', 0.05)]

    file_name = 'test_file.csv'

    # Вызываем функцию loading_clients
    loading_clients(clients_info, file_name, cur_mock)

    # Проверяем, что методы были вызваны с правильными аргументами
    cur_mock.execute.assert_called()
    cur_mock.fetchone.assert_called()
    cur_mock.execute.assert_has_calls([
        call("""
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
            AND interest_rate = %s""", ('John', 'Doe', '123 Main St', '555-1234', '2022-01-01', 'john.doe@example.com',
                                         1000, '2022-01-01', '2023-01-01', 0.05)),
        call(
            'INSERT INTO staging.clients (first_name, last_name, address, phone_number, registration_date, '
            'email, deposit_amount, opening_date, closing_date, interest_rate, file_name, timestamp_column)'
            ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
            ('John', 'Doe', '123 Main St', '555-1234', '2022-01-01', 'john.doe@example.com', 1000, '2022-01-01',
             '2023-01-01', 0.05, 'test_file.csv', ANY)
            ),
    ], any_order=True)
    # В случае использования timestamp_column вставляем ANY для проверки любого значения
    cur_mock.close.assert_not_called()  # Проверяем, что метод close не был вызван


def test_loading_liabilities_with_mocks_without_timestamp(cur_mock):
    """Тест для проверки функции loading_liabilities. Имитирует подключение к БД
     и проверяет правильность отправки запросов. Объект без параметра timestamp."""
    # Устанавливаем возвращаемое значение для cur_mock.fetchone
    cur_mock.fetchone.return_value = None  # Возвращаем None для имитации отсутствия совпадающей записи чтобы INSERT сработал

    # Подготовка данных для загрузки без timestamp_column
    liabilities_info = [('9995.90', '9789.01', '17890.12', '8901.23', '19012.34')]

    file_name = 'test_file.csv'

    # Вызываем функцию loading_liabilities
    loading_liabilities(liabilities_info, file_name, cur_mock)

    # Проверяем, что методы были вызваны с правильными аргументами
    cur_mock.execute.assert_called()
    cur_mock.fetchone.assert_called()
    cur_mock.execute.assert_has_calls([
        call('SELECT id FROM staging.control_liabilities '
             'WHERE financial_instruments_debts = %s '
             'AND securities_obligations = %s '
             'AND reporting_data = %s '
             'AND invoices_to_pay = %s '
             'AND funds_in_accounts = %s', ('9995.90', '9789.01', '17890.12', '8901.23', '19012.34')),
        call(
            'INSERT INTO staging.control_liabilities (financial_instruments_debts, securities_obligations,'
            ' reporting_data, invoices_to_pay, funds_in_accounts, file_name, timestamp_column)'
            ' VALUES (%s, %s, %s, %s, %s, %s, %s)',
            ('9995.90', '9789.01', '17890.12', '8901.23', '19012.34', 'test_file.csv', ANY)
            ),
    ], any_order=True)
    # В случае использования timestamp_column вставляем ANY для проверки любого значения
    cur_mock.close.assert_not_called()  # Проверяем, что метод close не был вызван


def test_loading_liabilities_with_mocks_with_timestamp(cur_mock):
    """Тест для проверки функции loading_liabilities. Имитирует подключение к БД
     и проверяет правильность отправки запросов. Объект с параметром timestamp."""
    # Устанавливаем возвращаемое значение для cur_mock.fetchone
    cur_mock.fetchone.return_value = None  # Возвращаем None для имитации отсутствия совпадающей записи чтобы INSERT сработал

    file_name = 'test_file.csv'

    # Подготовка данных для загрузки c timestamp_column
    liabilities_info2 = [('1111.56', '2345.67', '3456.78', '1111.89', '5678.90', '2024-02-10 00:00:00.000')]

    # Вызываем функцию loading_liabilities
    loading_liabilities(liabilities_info2, file_name, cur_mock)

    # Проверяем, что методы были вызваны с правильными аргументами
    cur_mock.execute.assert_called()
    cur_mock.fetchone.assert_called()
    cur_mock.execute.assert_has_calls([
        call('SELECT id FROM staging.control_liabilities '
             'WHERE financial_instruments_debts = %s '
             'AND securities_obligations = %s '
             'AND reporting_data = %s '
             'AND invoices_to_pay = %s '
             'AND funds_in_accounts = %s '
             'AND timestamp_column = %s', ('1111.56', '2345.67',
                                           '3456.78', '1111.89', '5678.90',
                                           '2024-02-10 00:00:00.000')),
        call(
            'INSERT INTO staging.control_liabilities (financial_instruments_debts, securities_obligations,'
            ' reporting_data, invoices_to_pay, funds_in_accounts, file_name, timestamp_column)'
            ' VALUES (%s, %s, %s, %s, %s, %s, %s)',
            ('1111.56', '2345.67', '3456.78', '1111.89', '5678.90', 'test_file.csv', '2024-02-10 00:00:00.000')
        ),
    ], any_order=True)

    cur_mock.close.assert_not_called()  # Проверяем, что метод close не был вызван


# Запускаем тест
if __name__ == '__main__':
    pytest.main()
