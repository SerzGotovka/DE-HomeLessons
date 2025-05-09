Задача 2 (усложнение задачи 5 из самостоятельного решения). Анализ загрузки самолетов по дням недели
Определить, в какие дни недели самолеты загружены больше всего.

WITH 
	numb_seat 	AS 
	(SELECT 
			a.aircraft_code,
		    a.model,
		    count(s.seat_no)                                        AS cnt_seat -- кол-во мест в самолете
	    FROM bookings.aircrafts a
	    JOIN bookings.seats s ON a.aircraft_code = s.aircraft_code
	    GROUP BY a.aircraft_code),
	
	seats_sold 	AS 
	(SELECT
			flight_id, 
			count(seat_no) 											AS seats_sold 		-- КОЛ-ВО ПРОДАННЫХ МЕСТ В САМОЛЕТЕ
	FROM bookings.boarding_passes GROUP BY flight_id)
	
SELECT 
    TO_CHAR(f.scheduled_departure, 'FMDay') 						AS day_of_week, -- дни недели
    round(avg(((ss.seats_sold::NUMERIC / ns.cnt_seat) * 100)), 2)	AS avg_loaded_airplanes  -- средняя загрузка самолетов для каждого дня.
FROM bookings.ticket_flights tf
JOIN bookings.flights f ON f.flight_id = tf.flight_id
JOIN numb_seat ns ON ns.aircraft_code = f.aircraft_code
JOIN seats_sold ss ON ss.flight_id = tf.flight_id
GROUP BY day_of_week;