Задача 1. Исследовать метод доступа (Seq Scan или Index Scan)
и понять, как фильтрация влияет на производительность.
Исследовать запрос:
SELECT * FROM flights WHERE status = 'delayed';
Создать индекс на поле и зафиксировать изменения
в скорости выполнения запроса и объема размера таблички



EXPLAIN analyze
SELECT * FROM bookings.flights WHERE status = 'Arrived';

CREATE INDEX idx_flights_status3 ON bookings.flights USING HASH (status)
CREATE INDEX idx_flights_status3 ON bookings.flights (status);



Задача 2. Подзапросы. Исследовать, как PostgreSQL обрабатывает подзапросы.
Результат: вывод о том как это работает

Задача 2. Подзапросы. Исследовать, как PostgreSQL обрабатывает подзапросы.

EXPLAIN analyze
SELECT *
FROM bookings.flights
WHERE flight_id IN (
    SELECT flight_id FROM bookings.ticket_flights WHERE amount > 1000
);



Задача 3. Самые популярные дни недели для вылетов
Определить, в какие дни недели чаще всего совершаются вылеты
Используемая таблица: bookings.flights

SELECT * FROM bookings.flights;

EXPLAIN analyze
SELECT 
    TO_CHAR(scheduled_departure, 'Day') AS day_of_week,  
    COUNT(*) AS flight_count                           
FROM 
    bookings.flights                                           
GROUP BY 
    day_of_week                                      
ORDER BY 
    flight_count DESC;     


Задача 4. Анализ сезонности продаж билетов
Определить месяцы с максимальным количеством проданных билетов (топ 3).
Используемые таблицы: bookings.bookings, bookings.tickets

SELECT 
	to_char(b.book_date, 'Month') AS monts_bookings,
	count(*) AS cnt_bookings
FROM bookings.bookings b 
JOIN bookings.tickets t ON b.book_ref = t.book_ref
GROUP BY monts_bookings
ORDER BY cnt_bookings DESC 
LIMIT 3;


Запрос 5: Расчет загрузки самолетов. 
Определить среднюю загрузку самолетов (процент занятых мест) для каждого рейса.

WITH 
	numb_seat 	AS 
	(SELECT 
			a.aircraft_code,
		    a.model,
		    count(s.seat_no) AS cnt_seat
	    FROM bookings.aircrafts a
	    JOIN bookings.seats s ON a.aircraft_code = s.aircraft_code
	    GROUP BY a.aircraft_code),
	
	seats_sold 	AS 
	(SELECT
			flight_id, 
			count(seat_no) 											AS seats_sold 		-- КОЛ-ВО ПРОДАННЫХ МЕСТ В САМОЛЕТЕ
	FROM bookings.boarding_passes GROUP BY flight_id)
	
SELECT 
    f.flight_no,						
    round(avg(((ss.seats_sold::NUMERIC / ns.cnt_seat) * 100)), 2)	AS avg_loaded_airplanes  -- средняя загрузка самолетов для каждого дня.
FROM bookings.ticket_flights tf
JOIN bookings.flights f ON f.flight_id = tf.flight_id
JOIN numb_seat ns ON ns.aircraft_code = f.aircraft_code
JOIN seats_sold ss ON ss.flight_id = tf.flight_id
GROUP BY f.flight_no;