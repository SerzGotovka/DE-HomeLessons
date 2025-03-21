"""
Задача 3.
Система анализа транзакций клиентов банка
Описание задачи:
Банк хочет создать систему для анализа транзакций своих клиентов. Каждый клиент имеет счет, на котором происходят транзакции (пополнения, списания). Банк хочет:
- Хранить информацию о клиентах и их транзакциях.
- Анализировать транзакции (например, считать общий доход, расход, баланс).
- Генерировать отчеты для клиентов.

Необходимо создать классы на Python с функционалом описанным выше:
Класс Client (Клиент):
- Хранит информацию о клиенте (имя, ID).
- Содержит список счетов клиента.
Класс Account (Счет):
- Хранит информацию о счете (номер счета, баланс).
- Содержит список транзакций.
Класс Transaction (Транзакция):
- Хранит информацию о транзакции (тип: доход/расход, сумма, дата).
Класс Bank (Банк):
- Управляет клиентами и счетами.
- Предоставляет методы для анализа данных (например, общий доход, расход, баланс).

Дополнительные задания для практики:
- Добавьте возможность фильтрации транзакций по дате.
- Реализуйте метод для поиска клиента по ID.
- Добавьте возможность экспорта транзакций в файл (например, CSV).
- Реализуйте класс для генерации отчетов (например, PDF или Excel).

! Важно само проектирование классов, данные можете подобрать сами
"""

from datetime import datetime


class Bank:
    def __init__(self):
        self.clients = {} # Словарь для хранения клиентов по их ID

    def add_client(self, client):
        """Добавление клиента"""
        client = self.clients[client.client_id]

    def get_client(self, client_id):
        """Получить клиента по id"""
        return self.clients.get(client_id)
    
    def total_income(self, client_id):
        client = self.get_client(client_id)

    


class Client:
    """Класс для создания клиентов"""

    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.accounts = []

    def get_clients(self):
        """Возвращает список счетов клиента."""
        return self.accounts

    def add_account(self, account):
        """Метод для добавления счета клиенту"""
        self.accounts.append(account)

    def __repr__(self):
        """Строковое представление для клиента"""
        return f"Clients(id={self.id}, name={self.name})"


class Transaction:
    """Класс для создания транзакций"""

    def __init__(self, type_transaction, amount, date=None):
        self.type_transaction = type_transaction
        self.amount = amount
        self.date = date if date else datetime.now()

    def __repr__(self):
        """Строковое представление транзакции."""
        return f"Transaction(type={self.type_transaction}, amount={self.amount}, date={self.date})"


class Account:
    """Класс для создания счетов"""

    def __init__(self, numb_acc, balance=0):
        self.numb_acc = numb_acc
        self.balance = balance
        self.transactions = []

    def add_transaction(self, transaction):
        """Добавление транзакций на счет"""
        if transaction.transaction_type == "income":
            self.balance += transaction.amount
        elif transaction.transaction_type == "expense":
            self.balance -= transaction.amount

    def get_balance(self):
        """Метод для получения текущего баланса"""
        return self.balance

    def get_transaction(self):
        """Метод для получения списка транзакций"""
        return self.transactions

    def __repr__(self):
        """Строковое представление счета."""
        return f"Transaction(number={self.numb_acc}, balance={self.balance}, )"
