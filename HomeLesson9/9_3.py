"""
Задача 3
Условие:
У вас есть два файла:
accounts.txt — содержит текущие балансы счетов клиентов. Формат: Имя:Баланс.
transactions.txt — содержит список транзакций, которые нужно применить к балансам. 
Формат: Имя:Сумма, где Сумма может быть положительной (пополнение) или отрицательной (списание).
Задание: Вывести в accounts.txt - итоговое значение счета
"""

accounts_dict = dict()
transaction_dict = dict()
total_balance = dict()

with open('HomeLesson9/accounts.txt', 'r', encoding='utf-8') as file:
    content = file.readlines()
    
    for account in content:
        name, balance = account.strip().split(':')

        accounts_dict[name] = int(balance)


with open('HomeLesson9/transactions.txt', 'r', encoding='utf-8') as file:
    content = file.readlines()

    for transaction in content:
        name, action = transaction.strip().split(':')
        if name in transaction_dict:
            transaction_dict[name] += int(action) 
        else:
            transaction_dict[name] = int(action)    
    print(transaction_dict)

total_balance = accounts_dict.copy()  # Инициализируем итоговые балансы копией текущих

for name, amount in transaction_dict.items():
    if name in total_balance:
        total_balance[name] += amount  # Обновляем баланс


with open('HomeLesson9/accounts.txt', 'w', encoding='utf-8') as file: 
        for name, total in total_balance.items():
            file.write(f'{name}:{total}\n')
