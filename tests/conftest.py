import os
import sys
import pytest
from unittest.mock import MagicMock


# Вставляем путь для доступа к нужным модулям
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '')))


# Определение фикстуры
@pytest.fixture
def cur_mock():
    # Создаем мок для объекта подключения к базе данных
    return MagicMock()


# Определение путей к файлам для запуска тестов в командной строке
path_clients = "tests/loading_test_files/clients_test.csv"
path_capital = "tests/loading_test_files/capital_test.csv"


# Определение путей к файлам для запуска тестов в PyCharm
# path_clients = "loading_test_files/clients_test.csv"
# path_capital = "loading_test_files/capital_test.csv"
