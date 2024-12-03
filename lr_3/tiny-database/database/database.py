from abc import ABC, abstractmethod
import math
import csv
import os


class SingletonMeta(type):
    
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class Database(metaclass=SingletonMeta):

    def __init__(self):
        self.tables = {}


    def registerTable(self, tableName, table):
        if tableName in self.tables:
            raise ValueError(f"Table {tableName} already exists.")
        self.tables[tableName] = table


    def isTableExist(self, tableName):
        table = self.tables.get(tableName)
        if table is None:
            raise ValueError(f"Table {tableName} does not exists.")
        return table


    def insert(self, tableName, data):
        table = self.isTableExist(tableName)
        table.insert(data)


    def select(self, tableName, attr=None, value=None, start=0, end=math.inf):
        """
        Выполняет выборку из таблицы с возможностью фильтрации по атрибуту.
        """
        table = self.isTableExist(tableName)

        if attr and value is not None:
            # Фильтрация по атрибуту
            return [
                record for record in table.select(start=start, end=end)
                if record.get(attr) == value
            ]
        # Выборка по диапазону
        return table.select(start=start, end=end)
    
    
    def load(self, tableName):
        table = self.isTableExist(tableName)
        table.load()


    """
    объединение выполняется для левой таблицы по id правой таблицы
    """
    def join(self, tableLeftName, tableRightName, joinAttr):
        tableLeft = self.isTableExist(tableLeftName)
        tableRight = self.isTableExist(tableRightName)
        
        leftTableRecords = tableLeft.data
        rightTableRecords = tableRight.data
        mergedTable = []
        
        for leftRecord in leftTableRecords:
            for rightRecord in rightTableRecords:
                if leftRecord[joinAttr] == rightRecord['id']:
                    rightRecordCopy = rightRecord.copy()
                    rightRecordCopy[joinAttr] = rightRecordCopy.pop('id')
                    mergedRecord = {**leftRecord, **rightRecordCopy}
                    mergedTable.append(mergedRecord)
                    break
        return mergedTable


class Table(ABC): #pragma: no cover
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


class BaseTable(Table):
    ATTRS = ()
    FILE_PATH = ''
    
    def __init__(self):
        self.data = []
        self.keys = set()
        self.load()

    def insert(self, data):
        entry = dict(zip(self.ATTRS, data.split()))
        entryKeys = self.get_entry_keys(entry)
        if entryKeys in self.keys:
            raise ValueError(
                f"Entry with keys {entryKeys} already exists."
            )

        self.data.append(entry)
        self.keys.add(entryKeys)
        self.save()

    def select(self, start, end):
        """
        Выбирает данные в указанном диапазоне по 'id'.
        """
        return [entry for entry in self.data if start <= int(entry['id']) <= end]

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
                    entryKeys = self.get_entry_keys(row)
                    if entryKeys in self.keys:
                        print(f"Entry with keys {entryKeys} already exists.")
                    else:
                        self.data.append(row)
                        self.keys.add(entryKeys)
        else:
            self.data = []
            self.keys = set()

    def get_entry_keys(self, entry):
        """
        Метод для определения уникального ключа записи.
        Должен быть переопределен в подклассах.
        """

class EmployeeTable(BaseTable):
    
    ATTRS = ('id', 'name', 'age', 'salary', 'department_id')
    FILE_PATH = 'employee_table.csv'

    def get_entry_keys(self, entry):
        # Уникальный ключ - пара (id, department_id)
        return (int(entry['id']), int(entry['department_id']))


class DepartmentTable(BaseTable):
    ATTRS = ('id', 'department_name')
    FILE_PATH = 'department_table.csv'

    def get_entry_keys(self, entry):
        # Уникальный ключ - только id
        return int(entry['id'])