from database.database import Database, EmployeeTable, DepartmentTable


if __name__ == "__main__" :
    db = Database()

    # Создание таблиц в базе данных
    db.register_table("employees", EmployeeTable())
    db.register_table("departments", DepartmentTable())

    # Вставка элементов
    db.insert("employees", "1 Alice 30 70000 1")
    db.insert("employees", "2 Bob 29 100000 1")
    db.insert("departments", "Engineering")

    print(db.select('employees', 1, 2))

