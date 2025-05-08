Задание 4.1: Рейсы с заполняемостью выше средней
Задача: Найти рейсы, где количество проданных билетов превышает среднее по всем рейсам.
Таблица: flights
Задание 4.2: Создать функцию с вводом параметра минимального процента заполняемости и выводом всех рейсов удовлетворяющих этому проценту


CREATE OR REPLACE FUNCTION bookings.get_flights_by_fill_percent(min_percent numeric)
RETURNS TABLE (
    flight_id integer,
    flight_no bpchar(6),
    --scheduled_departure timestamptz,
    --scheduled_arrival timestamptz,
    departure_airport bpchar(3),
    arrival_airport bpchar(3),
    status varchar(20),
    aircraft_code bpchar(3),
    fill_percent numeric
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        f.flight_id,
        f.flight_no,
        --f.scheduled_departure,
        --f.scheduled_arrival,
        f.departure_airport,
        f.arrival_airport,
        f.status,
        f.aircraft_code,
        (COUNT(tf.ticket_no)::numeric / total_seats.total_seats * 100) AS fill_percent  
    FROM 
        bookings.flights f
    JOIN 
        bookings.ticket_flights tf ON f.flight_id = tf.flight_id
    JOIN 
        (SELECT a.aircraft_code, COUNT(s.seat_no) AS total_seats
         FROM bookings.aircrafts a
         JOIN bookings.seats s ON a.aircraft_code = s.aircraft_code
         GROUP BY a.aircraft_code) AS total_seats 
    ON f.aircraft_code = total_seats.aircraft_code
    GROUP BY 
        f.flight_id, 
        f.flight_no, 
        f.scheduled_departure, 
        f.scheduled_arrival, 
        f.departure_airport, 
        f.arrival_airport, 
        f.status, 
        f.aircraft_code, 
        total_seats.total_seats
    HAVING 
        (COUNT(tf.ticket_no)::numeric / total_seats.total_seats * 100) >= min_percent;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM bookings.get_flights_by_fill_percent(70.0);