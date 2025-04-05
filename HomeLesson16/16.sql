1. Создание таблицы promocodes
Что нужно сделать:
Создать таблицу для хранения промокодов, которые могут применять пользователи к своим заказам.

Требования:
promo_id – уникальный ID промокода (первичный ключ, автоинкремент).
code – текст промокода (уникальный, не может повторяться).
discount_percent – процент скидки (от 1 до 100).
valid_from и valid_to – срок действия.
max_uses – максимальное количество применений (NULL = без ограничений).
used_count – сколько раз уже использован (по умолчанию 0).
is_active – активен ли промокод (1 или 0, по умолчанию 1).
created_by – кто создал промокод (внешний ключ на users.id).


CREATE TABLE IF NOT EXISTS users (
user_id INTEGER PRIMARY KEY AUTOINCREMENT,
username TEXT NOT NULL UNIQUE,
email TEXT NOT NULL UNIQUE,
age INTEGER CHECK(age BETWEEN 0 AND 120),
created_at DATETIME DEFAULT CURRENT_TIMESTAMP

);


CREATE TABLE promocodes (
	promo_id INTEGER PRIMARY KEY AUTOINCREMENT,
	code TEXT UNIQUE NOT NULL,
	dickount_percent INTEGER CHECK(dickount_percent BETWEEN 1 AND 100),
	valid_from DATETIME,
	valid_to DATETIME,
	max_uses INTEGER CHECK(max_uses > 0),
	used_count INTEGER DEFAULT 0 CHECK(used_count >= 0),
	is_active DEFAULT 1 CHECK(is_active IN (0,1)),
	created_by INTEGER,
	
	FOREIGN KEY (created_by) REFERENCES users(user_id) ON DELETE SET NULL
);


INSERT INTO users (username, email, age) VALUES
    ('ivan_petrov', 'ivan.p@example.com', 28),
    ('anna_smirnova', 'anna.s@example.com', 32),
    ('alex_volkov', 'alex.v@example.com', 25),
    ('elena_kuz', 'elena.k@example.com', 41),
    ('dmitry_sokolov', 'dmitry.s@example.com', 19),
    ('olga_ivanova', 'olga.i@example.com', 23),
    ('sergey_fedorov', 'sergey.f@example.com', 37),
    ('marina_orlova', 'marina.o@example.com', 29),
    ('pavel_novikov', 'pavel.n@example.com', 45),
    ('ekaterina_bez', 'ekaterina.b@example.com', 31);


INSERT INTO promocodes (code, dickount_percent, valid_from, valid_to, max_uses, created_by)
VALUES
	('SUMMER10', 10, '2023-06-01', '2025-08-31', 100, 1),
    ('WELCOME20', 20, '2023-01-01', '2025-12-31', NULL, 2),
    ('BLACKFRIDAY30', 30, '2023-11-24', '2023-11-27', 500, 3),
    ('NEWYEAR15', 15, '2023-12-20', '2024-01-10', 200, 4),
    ('FLASH25', 25, '2023-10-01', '2025-10-07', 50, 5),
    ('LOYALTY5', 5, '2023-01-01', '2024-01-01', NULL, 6),
    ('MEGA50', 50, '2023-09-01', '2023-09-30', 10, 7),
    ('AUTUMN20', 20, '2025-09-01', '2025-11-30', 300, 8),
    ('SPRING10', 10, '2023-03-01', '2023-05-31', 150, 9),
    ('VIP40', 40, '2025-07-01', '2025-07-31', 20, 10);


Анализ по группам скидок
Сгруппировать промокоды по диапазонам скидок и вывести:
- Количество промокодов в группе.
- Минимальную и максимальную скидку.
- Сколько из них имеют ограничение по использованию (max_uses IS NOT NULL).

SELECT 
	CASE
		WHEN dickount_percent BETWEEN 1 AND 10 THEN '1%-10%'
		WHEN dickount_percent BETWEEN 11 AND 20 THEN '11%-20%'
		WHEN dickount_percent BETWEEN 21 AND 30 THEN '21%-30%'
		WHEN dickount_percent BETWEEN 31 AND 40 THEN '31%-40%'
		WHEN dickount_percent BETWEEN 31 AND 40 THEN '41%-50%'
		WHEN dickount_percent >= 50 THEN '> 50%'	
	END AS discount_group,
	count(*) AS count_discount,
	MIN(dickount_percent) AS min_discount,
	MAX(dickount_percent) AS max_discount,
	SUM(CASE WHEN max_uses IS NOT NULL THEN 1 ELSE 0 END) AS count_max_uses_not_null
FROM promocodes
GROUP BY (discount_group);
	
	
4. Анализ по времени действия
Что нужно сделать:
Разделить промокоды на:
- Активные (текущая дата между valid_from и valid_to).
- Истекшие (valid_to < текущая дата).
- Еще не начавшиеся (valid_from > текущая дата).
Для каждой группы вывести:
- Количество промокодов.
- Средний процент скидки.
- Сколько из них имеют лимит использований.

SELECT 
	CASE 
		WHEN CURRENT_TIMESTAMP BETWEEN valid_from AND valid_to THEN 'Активные'
        --или
		--WHEN DATE('now') BETWEEN DATE(valid_from) AND DATE(valid_to) THEN 'Active'
		WHEN CURRENT_TIMESTAMP < valid_to THEN 'Истекшие'
		WHEN CURRENT_TIMESTAMP > valid_to THEN 'Еще не начавшиеся'
	END AS time_group,
	count(*) AS count_promocodes,
	ROUND(AVG(dickount_percent), 0) AS avg_promocodes,
	SUM(CASE WHEN max_uses IS NOT NULL THEN 1 ELSE 0 END) AS count_notnull_promocodes
FROM promocodes
GROUP BY time_group;