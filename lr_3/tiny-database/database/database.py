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
        UPD: если в качестве таблицы передано имя - выполняется поиск в БД
        иначе берётся переданная таблица
        UPD2: теперь таблицы не обладают методом select, он принадлежит
        только БД
        """
        # Если переданы имена таблиц, загружаем их данные
        if isinstance(tableName, str):
            table = self.isTableExist(tableName).data
        else:
            table = tableName

        # Фильтруем записи по диапазону 'id'
        selectedRecords = [
            record
            for record in table
            if start <= int(record.get("id", math.inf)) <= end
        ]

        # Если задана фильтрация по атрибуту
        if attr and value is not None:
            selectedRecords = [
                record for record in selectedRecords if record.get(attr) == value
            ]

        return selectedRecords

    def load(self, tableName):
        table = self.isTableExist(tableName)
        table.load()

    """
    объединение выполняется для левой таблицы по id правой таблицы
    UPD: если в качестве таблицы передано имя - выполняется поиск в БД
    иначе берётся переданная таблица
    """

    def join(self, tableLeft, tableRight, joinAttr):

        # Если переданы имена таблиц, загружаем их данные
        if isinstance(tableLeft, str):
            leftTableRecords = self.isTableExist(tableLeft).data
        else:
            leftTableRecords = tableLeft

        if isinstance(tableRight, str):
            rightTableRecords = self.isTableExist(tableRight).data
        else:
            rightTableRecords = tableRight

        mergedTable = []
        for leftRecord in leftTableRecords:
            for rightRecord in rightTableRecords:
                if leftRecord[joinAttr] == rightRecord["id"]:
                    rightRecordCopy = rightRecord.copy()
                    rightRecordCopy[joinAttr] = rightRecordCopy.pop("id")
                    mergedRecord = {**leftRecord, **rightRecordCopy}
                    mergedTable.append(mergedRecord)
                    break
        return mergedTable

    def aggregate(self, aggrMethod, attr, table):
        if not table:
            raise ValueError("The table is empty.")

        # Проверяем, что указанный атрибут существует в записях
        if not all(attr in entry for entry in table):
            raise ValueError(f"Attribute {attr} not found in table.")

        values = [entry[attr] for entry in table]

        match aggrMethod:

            case "avg":
                try:
                    numeric_value = [float(value) for value in values]
                except ValueError:
                    raise ValueError(
                        "Can't aggregate non-numeric value(-s)"
                        + " with average method."
                    )
                return (
                    f"Average {attr}: "
                    + f"{float(sum(numeric_value) / len(numeric_value))}."
                )

            case "max":
                return f"Maximum {attr}: {max(values)}."

            case "min":
                return f"Minimum {attr}: {min(values)}."

            case "count":
                return f"Count {attr}: {len(values)}."

            case _:
                raise ValueError(f"Can't find {aggrMethod} method.")


class Table(ABC):  # pragma: no cover
    """Абстрактный базовый класс для таблиц с вводом/выводом файлов CSV."""

    @abstractmethod
    def insert(self, data):
        pass

    @abstractmethod
    def load(self):
        pass


class BaseTable(Table):

    ATTRS = ()
    FILE_PATH = ""

    def __init__(self):
        self.data = []
        self.keys = set()
        self.load()

    def insert(self, data):
        entry = dict(zip(self.ATTRS, data.split()))
        entryKeys = self.get_entry_keys(entry)
        if entryKeys in self.keys:
            raise ValueError(f"Entry with keys {entryKeys} already exists.")
        self.data.append(entry)
        self.keys.add(entryKeys)
        self.save()

    def save(self):
        with open(self.FILE_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.ATTRS)
            writer.writeheader()
            writer.writerows(self.data)

    def load(self):
        if os.path.exists(self.FILE_PATH):
            with open(self.FILE_PATH, "r") as f:
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
    ATTRS = ("id", "name", "age", "salary", "department_id")
    FILE_PATH = "employee_table.csv"

    def get_entry_keys(self, entry):
        # Уникальный ключ - пара (id, department_id)
        return (int(entry["id"]), int(entry["department_id"]))


class DepartmentTable(BaseTable):
    ATTRS = ("id", "department_name")
    FILE_PATH = "department_table.csv"

    def get_entry_keys(self, entry):
        # Уникальный ключ - только id
        return int(entry["id"])


class SalesTable(BaseTable):
    ATTRS = ("id", "product_name", "price", "seller_id")
    FILE_PATH = "goods_table.csv"

    def get_entry_keys(self, entry):
        # Уникальный ключ - только id
        return int(entry["id"])
