Домашнее задание (нужно сделать 2 на выбор):
- Задача 1: Вывести аэропорты, из которых выполняется менее 50 рейсов
- Задача 2: Вывести среднюю стоимость билетов для каждого маршрута (город вылета - город прилета)
- Задача 3: Вывести топ-5 самых загруженных маршрутов (по количеству проданных билетов)
- Задача 4: Найти пары рейсов, вылетающих из одного аэропорта в течение 1 часа
Подсказка: (f1.scheduled_departure - f2.scheduled_departure))) <= 3600


-- Задача 1: Вывести аэропорты, из которых выполняется менее 50 рейсов
select 
	departure_airport,
	count(*)
from bookings.flights
group by departure_airport
having count(*) < 50;


-- Задача 2: Вывести среднюю стоимость билетов для каждого маршрута (город вылета - город прилета)
select
	ad.city AS departure_city,
    aa.city AS arrival_city,
    round(AVG(tf.amount)) AS average_price
from bookings.flights f 
join bookings.airports aa on f.arrival_airport = aa.airport_code
join bookings.airports ad on f.departure_airport = ad.airport_code
join bookings.ticket_flights tf on f.flight_id = tf.flight_id
group by ad.city, aa.city;


-- Задача 3: Вывести топ-5 самых загруженных маршрутов (по количеству проданных билетов)
select
	ad.city AS departure_city,
    aa.city AS arrival_city,
    count(tf.ticket_no) AS count_price
from bookings.flights f 
join bookings.airports aa on f.arrival_airport = aa.airport_code
join bookings.airports ad on f.departure_airport = ad.airport_code
join bookings.ticket_flights tf on f.flight_id = tf.flight_id
group by ad.city, aa.city
order by count_price desc
limit 5;


-- Задача 4: Найти пары рейсов, вылетающих из одного аэропорта в течение 1 часа
--Подсказка: (f1.scheduled_departure - f2.scheduled_departure))) <= 3600
SELECT 
    f1.flight_id AS flight1_id,
    f2.flight_id AS flight2_id,
    f1.departure_airport,
    f1.scheduled_departure AS flight1_departure_time,
    f2.scheduled_departure AS flight2_departure_time
FROM 
    bookings.flights f1
JOIN 
    bookings.flights f2 ON f1.departure_airport = f2.departure_airport
WHERE 
    f1.flight_id <> f2.flight_id  -- Исключаем пары одного и того же рейса
    AND ABS(EXTRACT(EPOCH FROM (f1.scheduled_departure - f2.scheduled_departure))) <= 3600;

-- ИЛИ

SELECT f1.flight_id AS flight1, f2.flight_id AS flight2, f1.departure_airport
FROM bookings.flights f1
JOIN bookings.flights f2 ON f1.departure_airport = f2.departure_airport
               AND f1.flight_id <> f2.flight_id
               AND ABS(EXTRACT(EPOCH FROM (f1.scheduled_departure - f2.scheduled_departure))) <= 3600;