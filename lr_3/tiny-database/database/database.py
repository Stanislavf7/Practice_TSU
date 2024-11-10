from abc import ABC, abstractmethod
import csv
import os


class SingletonMeta(type):
    """ Синглтон метакласс для Database. """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class Database(metaclass=SingletonMeta):
    """ Класс-синглтон базы данных с таблицами, хранящимися в файлах. """

    def __init__(self):
        self.tables = {}

    def register_table(self, table_name, table):
        self.tables[table_name] = table

    def insert(self, table_name, data):
        table = self.tables.get(table_name)
        if table:
            table.insert(data)
        else:
            raise ValueError(f"Table {table_name} does not exist.")

    def select(self, table_name, *args):
        table = self.tables.get(table_name)
        return table.select(*args) if table else None
    
    def load(self, table_name):
        table = self.tables.get(table_name)
        table.load()


    """
    по умолчанию данный join выполняет объединение таблиц по заданию
    так же можно выполнять join с другими таблицами если элементы
    table_left являются id-элементом для table_right
    """
    def join(self, table_left='employees', table_right='departments', join_attr='department_id'):
        Left_data = self.tables[table_left].data
        Right_data = self.tables[table_right].data
        joined_data = []
        for Left_record in Left_data:
            for Right_record in Right_data:
                if Left_record[join_attr] == Right_record['id']:
                    Right_record_copy = Right_record.copy()
                    Right_record_copy[join_attr] = Right_record_copy.pop('id')
                    merged_record = {**Left_record, **Right_record_copy}
                    joined_data.append(merged_record)
                    break
        return joined_data


class Table(ABC):
    """ Абстрактный базовый класс для таблиц с вводом/выводом файлов CSV. """

    @abstractmethod
    def insert(self, data):
        pass

    @abstractmethod
    def select(self, *args):
        pass
    
    @abstractmethod
    def load(self):
        pass


class EmployeeTable(Table):
    """ Таблица сотрудников с методами ввода-вывода из файла CSV. """
    ATTRS = ('id', 'name', 'age', 'salary', 'department_id')
    FILE_PATH = 'employee_table.csv'

    def __init__(self):
        self.data = []
        self.load()  # Подгружаем из CSV-файла сразу при инициализации

    def insert(self, data):
        entry = dict(zip(self.ATTRS, data.split()))
        employee_info = self.select(int(entry['id']), int(entry['id']))
        if (employee_info and
            any(info['department_id'] == entry['department_id'] for info in employee_info)):
                raise ValueError(f'Entry with id: {entry['id']} '
                                 f'and department_id: {entry["department_id"]} already used')
        else:
            self.data.append(entry)
            self.save()

    def select(self, start_id, end_id):
        return [entry for entry in self.data if start_id <= int(entry['id']) <= end_id]

    def save(self):
        with open(self.FILE_PATH, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.ATTRS)
            writer.writeheader()
            writer.writerows(self.data)

    def load(self):
        if os.path.exists(self.FILE_PATH):
            with open(self.FILE_PATH, 'r') as f:
                reader = csv.DictReader(f)
                self.data = [row for row in reader]
        else:
            self.data = []


class DepartmentTable(Table):
    """ Таблица подразделенией с вводом-выводом в/из CSV файла. """
    
    """ Таблица строится с автоикрементом id"""
        
    ATTRS = ('id', 'department_name')
    FILE_PATH = 'department_table.csv'

    def __init__(self):
        self.next_id = 1
        self.data = []
        self.load()

    def select(self, department_name):
        return [row for row in self.data if department_name == row['department_name']]

    def insert(self, data):
        entry = {'id': str(self.next_id), 'department_name': data}
        self.data.append(entry)
        self.next_id += 1
        self.save()

    def save(self):
        with open(self.FILE_PATH, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.ATTRS)
            writer.writeheader()
            writer.writerows(self.data)

    def load(self):
        if os.path.exists(self.FILE_PATH):
            with open(self.FILE_PATH, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.data.append(row)
                    self.next_id += 1
        else:
            self.data = []
