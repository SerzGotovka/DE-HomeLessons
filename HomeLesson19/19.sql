Домашнее задание 19:
- Доделать задания и отправить все 3 мне на проверку
- Изучить тему временных таблиц - сделать все возможные операции с ней (создать, заполнить, сджойнить, удалить) результат исследования и скрипты прикрепить в домашке

ДОП. ДЗ:
- Необходимо самостоятельно изучить триггерные функции, создать материализованное представление для таблицы и настроить триггер, который будет срабатывать при обновлении или вставке данных в таблицу, после чего выполнять обновление материализованного представления.


Задача 1 для самостоятельного задания: 
Вам нужно проанализировать данные о продажах билетов, чтобы получить статистику в следующих разрезах:
- По классам обслуживания (fare_conditions)
- По месяцам вылета
- По аэропортам вылета
- По комбинациям: класс + месяц, класс + аэропорт, месяц + аэропорт
- Общие итоги

Используемые таблицы:
- ticket_flights (информация о билетах)
- flights (информация о рейсах)
- airports (информация об аэропортах)


SELECT 
    tf.fare_conditions AS class,
    EXTRACT(MONTH FROM subquery.actual_departure) AS month,
    subquery.airport_name AS departure_airport,
    COUNT(*) AS ticket_count,
    SUM(tf.amount) AS total_revenue
FROM 
    bookings.ticket_flights tf
JOIN 
    (SELECT 
         f.flight_id,
         f.actual_departure,
         a.airport_name
     FROM 
         bookings.flights f
     JOIN 
         bookings.airports a ON f.departure_airport = a.airport_code) AS subquery
ON 
    tf.flight_id = subquery.flight_id
GROUP BY 
    GROUPING SETS (
        (tf.fare_conditions), 
        (EXTRACT(MONTH FROM subquery.actual_departure)), 
        (subquery.airport_name), 
        (tf.fare_conditions, EXTRACT(MONTH FROM subquery.actual_departure)), 
        (tf.fare_conditions, subquery.airport_name), 
        (EXTRACT(MONTH FROM subquery.actual_departure), subquery.airport_name), 
        () 
    );
-------------------------------------------------------------------------------------------

Задача 2 для самостоятельного задания: 
Рейсы с задержкой больше средней (CTE + подзапрос)
Найдите рейсы, задержка которых превышает среднюю задержку по всем рейсам.

Используемые таблицы:
- flights

WITH avg_delay AS (
    SELECT 
        round(AVG(EXTRACT(EPOCH FROM (f.actual_departure - f.scheduled_departure)) / 60), 2) AS avg_delay_minutes
    FROM 
        bookings.flights f
    WHERE 
        f.actual_departure IS NOT NULL 
        AND f.scheduled_departure IS NOT NULL
)
SELECT
    flight_id,
    AVG(EXTRACT(EPOCH FROM (actual_arrival - scheduled_arrival)) / 60) AS avg_delay_per_flight,
    (SELECT avg_delay_minutes FROM avg_delay) AS total_avg_delay
FROM 
    bookings.flights
WHERE 
    EXTRACT(EPOCH FROM (actual_arrival - scheduled_arrival)) / 60 > (SELECT avg_delay_minutes FROM avg_delay)
GROUP BY 
    flight_id;
------------------------------------------------------------------------------------------------------------------------
Задача 3 для самостоятельного задания:
Создайте представление, которое содержит все рейсы, вылетающие из Москвы.


create view bookings.flight_from_moscow as
select 
	f.flight_id 
from bookings.flights f 
where f.departure_airport in (select airport_code from bookings.airports where city='Москва')

-----------------------------------------------------------------------------------------------------------------------------
ДОП. ДЗ:
Изучить тему временных таблиц - сделать все возможные операции
с ней (создать, заполнить, сджойнить, удалить) результат исследования и скрипты прикрепить в домашке


CREATE TEMPORARY TABLE temp_snapshot_bookings_seats AS
SELECT * FROM bookings.seats;							-- создание временной таблицы на основе другой таблицы (со значениями)

select * from temp_snapshot_bookings_seats;				

create TEMP TABLE positions 							-- создание временной таблицы positions
(
    id	INT PRIMARY KEY,
    title	VARCHAR,
    salary	INT
);

CREATE temp TABLE persons 								-- создание временной таблицы persons
(
    id	INT PRIMARY KEY,
    name	VARCHAR,
    position_id	INT,
    FOREIGN KEY (position_id) REFERENCES positions(id)
);

INSERT INTO positions (id, title, salary) values  		-- заполнение данными временной таблицы positions
	('1', 'Программист', '1500'),
	('2', 'Юрист', '700'),
	('3', 'HR', '700'),
	('4', 'Дизайнер', '700'),
	('5', 'Маркетолог', '500'),
	('6', 'Data Engineer', '3000');

INSERT INTO persons (id, name, position_id) values		-- заполнение данными временной таблицы persons
	('1', 'Владимир', '4'),
	('2', 'Алёна', '1'),
	('3', 'Евгений', '5'),
	('4', 'Артём', '2'),
	('5', 'Борис', '4'),
	('6', 'Татьяна', '3');

select * from positions;								-- вывести все данные из временной таблицы positions 
select * from persons;									-- вывести все данные из временной таблицы persons


select * from positions po
join persons pe on po.id = pe.position_id;				-- join временной таблицы positions с временной таблицей persons


select * from bookings.aircrafts a						-- join обычной табблицы booking.aircrafts c временной таблицой positions 
cross join positions p;

DROP TABLE IF EXISTS persons; 							-- удаление временной таблицы


CREATE TEMP TABLE temp_bookings_aircrafts 
LIKE bookings.aircrafts INCLUDING ALL;					-- Создаём временную таблицу со структурой, идентичной структуре существующей таблицы