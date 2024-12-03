import pytest
import os
import tempfile
from database.database import Database, EmployeeTable
from database.database import DepartmentTable, SalesTable


@pytest.fixture
def temp_employee_file():
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)


@pytest.fixture
def temp_department_file():
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)


@pytest.fixture
def temp_sales_file():
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)


@pytest.fixture
def database(temp_employee_file, temp_department_file, temp_sales_file):
    """Данная фикстура задает БД и определяет таблицы."""
    db = Database()
    db.tables.clear()

    # Используем временные файлы для тестирования файлового ввода-вывода
    employee_table = EmployeeTable()
    employee_table.FILE_PATH = temp_employee_file
    department_table = DepartmentTable()
    department_table.FILE_PATH = temp_department_file
    sales_table = SalesTable()
    sales_table.FILE_PATH = temp_sales_file

    db.registerTable("employees", employee_table)
    db.registerTable("departments", department_table)
    db.registerTable("sales", sales_table)

    yield db


"""
группа тестов на работу с таблицами
"""


def test_with_not_real_table(database):
    # проверяем исключение
    with pytest.raises(ValueError) as excinfo:
        database.insert("test", "test-data")
    assert str(excinfo.value) == "Table test does not exists."


def test_insert_same_table(database):
    with pytest.raises(ValueError) as excinfo:
        testTable = EmployeeTable()
        database.registerTable("employees", testTable)
    assert str(excinfo.value) == "Table employees already exists."


"""
группа тестов на работу с таблицей employees
"""


def test_employee(database):
    database.insert("employees", "1 Alice 30 70000 1")
    database.insert("employees", "2 Bob 28 60000 1")
    database.insert("employees", "1 Alice 30 70000 2")

    employee_data = database.select("employees", start=1, end=2)

    # проверяем работу insert и select
    assert len(employee_data) == 3
    assert employee_data[0] == {
        "id": "1",
        "name": "Alice",
        "age": "30",
        "salary": "70000",
        "department_id": "1",
    }
    assert employee_data[1] == {
        "id": "2",
        "name": "Bob",
        "age": "28",
        "salary": "60000",
        "department_id": "1",
    }
    assert employee_data[2] == {
        "id": "1",
        "name": "Alice",
        "age": "30",
        "salary": "70000",
        "department_id": "2",
    }

    # проверяем уникальность значений в таблице
    with pytest.raises(ValueError) as excinfo:
        database.insert("employees", "1 Charlie 22 50000 1")
    assert str(excinfo.value) == "Entry with keys (1, 1) already exists."


def test_read_employee(database, temp_employee_file):
    with open(temp_employee_file, "w") as f:
        f.write("id,name,age,salary,department_id\n")
        f.write("1,Alice,30,70000,1\n")
        f.write("2,Bob,28,60000,1\n")
        # значение, которое не попадёт в таблицу
        f.write("2,Charlie,22,50000,1\n")

    database.load("employees")
    employee_data = database.select("employees")

    # проверяем чтение из файла
    assert len(employee_data) == 2
    assert employee_data[0] == {
        "id": "1",
        "name": "Alice",
        "age": "30",
        "salary": "70000",
        "department_id": "1",
    }
    assert employee_data[1] == {
        "id": "2",
        "name": "Bob",
        "age": "28",
        "salary": "60000",
        "department_id": "1",
    }


"""
группа тестов на работу с таблицей department
"""


def test_department(database):
    database.insert("departments", "1 HR")
    database.insert("departments", "2 Finance")
    database.insert("departments", "3 Marketing")
    database.insert("departments", "4 IT")
    database.insert("departments", "5 Finance")

    department_data = database.select(
        "departments", attr="department_name", value="Finance"
    )

    # проверяем работу insert и select
    assert len(department_data) == 2
    assert department_data[0] == {"id": "2", "department_name": "Finance"}
    assert department_data[1] == {"id": "5", "department_name": "Finance"}

    # проверяем уникальность значений в таблице
    with pytest.raises(ValueError) as excinfo:
        database.insert("departments", "3 Engineering")
    assert str(excinfo.value) == "Entry with keys 3 already exists."


def test_read_department(database, temp_department_file):
    with open(temp_department_file, "w") as f:
        f.write("id,department_name\n")
        f.write("1,HR\n")
        f.write("2,Finance\n")
        f.write("3,Marketing\n")
        f.write("4,IT\n")
        f.write("5,Finance\n")
        f.write("3,Engineering")  # значение, которое не попадёт в таблицу

    database.load("departments")
    department_data = database.select("departments")

    # проверяем чтение из файла
    assert len(department_data) == 5
    assert department_data[0] == {"id": "1", "department_name": "HR"}
    assert department_data[1] == {"id": "2", "department_name": "Finance"}
    assert department_data[2] == {"id": "3", "department_name": "Marketing"}
    assert department_data[3] == {"id": "4", "department_name": "IT"}
    assert department_data[4] == {"id": "5", "department_name": "Finance"}


"""
группа тестов на работу с таблицей sales
"""


def test_sales(database):
    database.insert("sales", "1 Smartphone 29900 1")
    database.insert("sales", "2 Smartphone 59900 1")
    database.insert("sales", "3 Headphones 14490 3")
    database.insert("sales", "4 Laptop 69900 2")

    sales_data = database.select("sales", attr="product_name", value="Smartphone")

    # проверяем работу insert и select
    assert len(sales_data) == 2
    assert sales_data[0] == {
        "id": "1",
        "product_name": "Smartphone",
        "price": "29900",
        "seller_id": "1",
    }
    assert sales_data[1] == {
        "id": "2",
        "product_name": "Smartphone",
        "price": "59900",
        "seller_id": "1",
    }

    # проверяем уникальность значений в таблице
    with pytest.raises(ValueError) as excinfo:
        database.insert("sales", "1 Camera 26900 2")
    assert str(excinfo.value) == "Entry with keys 1 already exists."


def test_read_sales(database, temp_sales_file):
    with open(temp_sales_file, "w") as f:
        f.write("id,product_name,price,seller_id\n")
        f.write("1,Smartphone,29900,1\n")
        f.write("2,Smartphone,59900,1\n")
        f.write("3,Headphones,14490,3\n")
        f.write("4,Laptop,69900,2\n")
        f.write("1,Camera,26900,2\n")  # значение, которое не попадёт в таблицу

    database.load("sales")
    sales_data = database.select("sales", attr="product_name", value="Smartphone")

    # проверяем чтение из файла
    assert len(sales_data) == 2
    assert sales_data[0] == {
        "id": "1",
        "product_name": "Smartphone",
        "price": "29900",
        "seller_id": "1",
    }
    assert sales_data[1] == {
        "id": "2",
        "product_name": "Smartphone",
        "price": "59900",
        "seller_id": "1",
    }


"""
группа тестов методов database
"""


def test_join_employees_departments(database):
    database.insert("employees", "1 Alice 30 70000 1")
    database.insert("employees", "2 Bob 28 60000 2")
    database.insert("employees", "3 Charlie 28 71000 1")

    database.insert("departments", "1 HR")
    database.insert("departments", "2 Finance")
    database.insert("departments", "3 Marketing")

    joined_data = database.join("employees", "departments", "department_id")

    # проверяем объединение таблиц
    assert len(joined_data) == 3
    assert joined_data[0] == {
        "id": "1",
        "name": "Alice",
        "age": "30",
        "salary": "70000",
        "department_id": "1",
        "department_name": "HR",
    }
    assert joined_data[1] == {
        "id": "2",
        "name": "Bob",
        "age": "28",
        "salary": "60000",
        "department_id": "2",
        "department_name": "Finance",
    }
    assert joined_data[2] == {
        "id": "3",
        "name": "Charlie",
        "age": "28",
        "salary": "71000",
        "department_id": "1",
        "department_name": "HR",
    }


def test_double_join_with_aggregate(database):
    database.insert("employees", "1 Alice 30 70000 3")
    database.insert("employees", "2 Bob 28 60000 3")
    database.insert("employees", "3 Charlie 28 71000 3")

    database.insert("departments", "1 HR")
    database.insert("departments", "2 Finance")
    database.insert("departments", "3 Marketing")

    database.insert("sales", "1 Smartphone 29900 1")
    database.insert("sales", "2 Smartphone 59900 1")
    database.insert("sales", "3 Headphones 19900 1")
    database.insert("sales", "4 Headphones 14490 3")
    database.insert("sales", "5 Headphones 17490 3")

    joined_data = database.join("employees", "departments", "department_id")
    double_joined_data = database.join("sales", joined_data, "seller_id")

    joined_data_2 = database.join("sales", "employees", "seller_id")
    double_joined_data_2 = database.join(joined_data_2, "departments", "department_id")
    # проверяем разные варианты объединения
    assert double_joined_data == double_joined_data_2

    select_f_dbl_joined_data = database.select(
        double_joined_data, attr="product_name", value="Smartphone"
    )

    # проверяем объединение таблиц
    assert len(select_f_dbl_joined_data) == 2
    assert select_f_dbl_joined_data[0] == {
        "id": "1",
        "product_name": "Smartphone",
        "price": "29900",
        "seller_id": "1",
        "name": "Alice",
        "age": "30",
        "salary": "70000",
        "department_id": "3",
        "department_name": "Marketing",
    }
    assert select_f_dbl_joined_data[1] == {
        "id": "2",
        "product_name": "Smartphone",
        "price": "59900",
        "seller_id": "1",
        "name": "Alice",
        "age": "30",
        "salary": "70000",
        "department_id": "3",
        "department_name": "Marketing",
    }

    # проверяем агрегацию (все варианты) объединёной таблицы
    aggregate_data = database.aggregate("avg", "price", double_joined_data)
    assert aggregate_data == "Average price: 28336.0."

    aggregate_data = database.aggregate("min", "price", double_joined_data)
    assert aggregate_data == "Minimum price: 14490."

    aggregate_data = database.aggregate("max", "price", double_joined_data)
    assert aggregate_data == "Maximum price: 59900."

    aggregate_data = database.aggregate("count", "price", double_joined_data)
    assert aggregate_data == "Count price: 5."

    with pytest.raises(ValueError) as excinfo:
        aggregate_data = database.aggregate(
            "test", "price", double_joined_data
        )  # такой агрегации нет
    assert str(excinfo.value) == "Can't find test method."


def test_aggregation(database):
    # проверяем исключения методов агрегации
    test_table = []
    with pytest.raises(ValueError) as excinfo:
        aggregate_data = database.aggregate("avg", "price", test_table)
    assert str(excinfo.value) == "The table is empty."

    test_table = [
        {"name": "Test", "info": "This is test string"},
        {"name": "Second", "info": "This is second test string"},
    ]
    with pytest.raises(ValueError) as excinfo:
        aggregate_data = database.aggregate("avg", "price", test_table)
    assert str(excinfo.value) == "Attribute price not found in table."

    with pytest.raises(ValueError) as excinfo:
        aggregate_data = database.aggregate("avg", "name", test_table)
    assert (
        str(excinfo.value)
        == "Can't aggregate non-numeric value(-s) with average method."
    )

    aggregate_data = database.aggregate("max", "name", test_table)
    assert aggregate_data == "Maximum name: Test."
