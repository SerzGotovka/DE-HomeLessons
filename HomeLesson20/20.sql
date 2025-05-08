Задание 3.1: Анализ задержек рейсов по аэропорту
Задача: Для указанного аэропорта (по коду аэропорта) вывести статистику задержек.
Таблица: flights

Задание 3.2: Обернуть в функцию c вводом кода аэропорта



CREATE OR REPLACE FUNCTION get_flight_delays_by_airport(airport_code_input TEXT)
RETURNS TABLE (
    airport_code TEXT,
    airport_name TEXT,
    total_flights BIGINT, 
    avg_delay_minutes NUMERIC,
    max_delay_minutes NUMERIC,
    min_delay_minutes NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        f.departure_airport::TEXT AS airport_code, 
        a.airport_name AS airport_name,
        COUNT(*) AS total_flights, 
        AVG(EXTRACT(EPOCH FROM (f.actual_departure - f.scheduled_departure)) / 60) AS avg_delay_minutes,
        MAX(EXTRACT(EPOCH FROM (f.actual_departure - f.scheduled_departure)) / 60) AS max_delay_minutes,
        MIN(EXTRACT(EPOCH FROM (f.actual_departure - f.scheduled_departure)) / 60) AS min_delay_minutes
    FROM 
        bookings.flights f
    JOIN 
        bookings.airports a
    ON 
        f.departure_airport = a.airport_code
    WHERE 
        f.departure_airport = airport_code_input
        AND f.actual_departure IS NOT NULL
        AND f.scheduled_departure IS NOT NULL
    GROUP BY 
        f.departure_airport, a.airport_name;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM get_flight_delays_by_airport('ABK')