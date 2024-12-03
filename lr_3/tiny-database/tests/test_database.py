import pytest
import os
import tempfile
from database.database import Database, EmployeeTable, DepartmentTable

@pytest.fixture
def temp_employee_file():
    """ Создаем временный файл для таблицы рабочих """
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)

@pytest.fixture
def temp_department_file():
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    yield temp_file.name
    os.remove(temp_file.name)

#Пример, как используются фикстуры
@pytest.fixture
def database(temp_employee_file, temp_department_file):
    """ Данная фикстура задает БД и определяет таблицы. """
    db = Database()
    db.tables.clear()

    # Используем временные файлы для тестирования файлового ввода-вывода в EmployeeTable и DepartmentTable
    employee_table = EmployeeTable()
    employee_table.FILE_PATH = temp_employee_file
    department_table = DepartmentTable()
    department_table.FILE_PATH = temp_department_file

    db.registerTable("employees", employee_table)
    db.registerTable("departments", department_table)

    yield db

def test_insert_employee(database):
    database.insert("employees", "1 Alice 30 70000 1")
    database.insert("employees", "2 Bob 28 60000 1")
    database.insert("employees", "1 Alice 30 70000 2")
    
    employee_data = database.select("employees", start=1, end=2)
    
    #проверяем работу insert и select
    assert len(employee_data) == 3
    assert employee_data[0] == {'id': '1', 'name': 'Alice', 'age': '30', 'salary': '70000', 'department_id': '1'}
    assert employee_data[1] == {'id': '2', 'name': 'Bob', 'age': '28', 'salary': '60000', 'department_id': '1'}
    assert employee_data[2] == {'id': '1', 'name': 'Alice', 'age': '30', 'salary': '70000', 'department_id': '2'}
    
def test_insert_in_not_real_table(database):
    #проверяем исключение
    with pytest.raises(ValueError) as excinfo:  
        database.insert("test", "test-data")
    assert str(excinfo.value) == "Table test does not exists."

def test_insert_employee_x2(database):
    #проверяем исключение
    database.insert("employees", "1 Alice 30 70000 1")
    with pytest.raises(ValueError) as excinfo:
        database.insert("employees", "1 Alice 30 70000 1")
    assert str(excinfo.value) == "Entry with keys (1, 1) already exists."

def test_uniq_insert_department(database):
    database.insert("departments","1 HR")
    database.insert("departments","2 Finance")
    database.insert("departments","3 Marketing")
    database.insert("departments","4 IT")
    database.insert("departments","5 Finance")
    
    department_data = database.select("departments", attr = 'department_name', value = 'Finance')
    
    #проверяем уникальность записей, работу insert и select
    assert len(department_data) == 2
    assert department_data[0] == {'id': '2', 'department_name': 'Finance'}
    assert department_data[1] == {'id': '5', 'department_name': 'Finance'}
    
    #пробуем вставить запись с уже существующим id
    with pytest.raises(ValueError) as excinfo:
        database.insert("departments", "3 Engineering")
    assert str(excinfo.value) == "Entry with keys 3 already exists."

def test_join_employees_departments(database):
    database.insert("employees", "1 Alice 30 70000 1")
    database.insert("employees", "2 Bob 28 60000 2")
    database.insert("employees", "3 Charlie 28 71000 1")
    
    database.insert("departments","1 HR")
    database.insert("departments","2 Finance")
    database.insert("departments","3 Marketing")
    
    joined_data = database.join("employees", "departments", "department_id")
    
    #проверяем объединение таблиц
    assert len(joined_data) == 3
    assert joined_data[0] == {'id': '1', 'name': 'Alice', 'age': '30', 'salary': '70000', 
                              'department_id': '1', 'department_name': 'HR'}
    assert joined_data[1] == {'id': '2', 'name': 'Bob', 'age': '28', 'salary': '60000', 
                              'department_id': '2', 'department_name': 'Finance'}
    assert joined_data[2] == {'id': '3', 'name': 'Charlie', 'age': '28', 'salary': '71000', 
                              'department_id': '1', 'department_name': 'HR'}
    
def test_read_employee_data(database, temp_employee_file):
    with open(temp_employee_file, 'w') as f:
        f.write("id,name,age,salary,department_id\n")
        f.write("1,Alice,30,70000,1\n")
        f.write("2,Bob,28,60000,1\n")
        f.write("2,Charlie,22,50000,1\n")
    
    database.load("employees") 
    employee_data = database.select("employees", start = 1, end = 2)
    
    #проверяем чтение из файла
    assert len(employee_data) == 2
    assert employee_data[0] == {'id': '1', 'name': 'Alice', 'age': '30', 'salary': '70000', 'department_id': '1'}
    assert employee_data[1] == {'id': '2', 'name': 'Bob', 'age': '28', 'salary': '60000', 'department_id': '1'}

def test_read_department_data(database, temp_department_file):
    with open(temp_department_file, 'w') as f:
        f.write("id,department_name\n")
        f.write("1,HR\n")
        f.write("2,Finance\n")
        f.write("3,Marketing\n")
        f.write("4,IT\n")
        f.write("5,Finance\n")
        f.write("3,Engineering")
    
    database.load("departments")
    department_data = database.select("departments", attr = 'department_name', value = 'Finance')
    
    #проверяем чтение из файла
    assert len(department_data) == 2
    assert department_data[0] == {'id': '2', 'department_name': 'Finance'}
    assert department_data[1] == {'id': '5', 'department_name': 'Finance'}
    
    
def test_insert_same_table(database):
    with pytest.raises(ValueError) as excinfo:
        testTable = EmployeeTable()
        database.registerTable("employees", testTable)
        
    assert str(excinfo.value) == "Table employees already exists."