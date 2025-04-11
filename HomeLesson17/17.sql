Задача 1: Анализ распределения мест в самолетах Необходимо проанализировать распределение мест в самолетах по классам обслуживания. Рассчитать:
 - Общее количество мест в каждом самолете
  - Количество мест каждого класса
   - Процентное соотношение классов
    - Массив всех мест для каждого самолета 


-- Общее количество мест в каждом самолете
-- Массив всех мест для каждого самолета

select a.model,
       count(s.seat_no), (array_agg(s.seat_no))
from bookings.seats s
join bookings.aircrafts a on a.aircraft_code = s.aircraft_code
group by a.model;

-- Количество мест каждого класса
-- Процентное соотношение классов

select s.fare_conditions,
       count(s.seat_no),
       (round(count(s.seat_no)*100/SUM(COUNT(s.seat_no)) OVER ())) AS relations_classes
from bookings.seats s
join bookings.aircrafts a on a.aircraft_code = s.aircraft_code
group by s.fare_conditions;


Задача 2: Анализ стоимости билетов по рейсам Для каждого рейса рассчитать:
 - Минимальную, максимальную и среднюю стоимость билета
  - Разницу между самым дорогим и самым дешевым билетом
   - Медианную стоимость билета
    - Массив всех цен на билеты

select flight_id,
       min(amount) as min_amount,
       max(amount) as max_amount,
       round(avg(amount)) as avg_amount,
       PERCENTILE_CONT(0.5) within group (order by amount), 
       array_agg(amount),
       (max(amount)- min(amount)) as difference_amount
from bookings.ticket_flights
group by flight_id;


Задача 3: Статистика по бронированиям по месяцам - Проанализировать бронирования по месяцам: 
- Количество бронирований 
- Общую сумму бронирований 
- Средний чек 
- Массив всех сумм бронирований для анализа распределения

select to_char(book_date, 'Month') as booking_month,
       count(*) as count_booking,
       sum(total_amount) as total_amount,
       round(avg(total_amount)) as avg_amount,
       array_agg(total_amount)
from bookings.bookings
group by to_char(book_date, 'Month'); 

Задача 4: Анализ пассажиропотока по аэропортам - Рассчитать для каждого аэропорта: 
- Общее количество вылетов 
- Количество уникальных аэропортов назначения 
- Массив всех аэропортов назначения 


SELECT departure_airport,
       count(departure_airport) as count_departure,
       COUNT(DISTINCT arrival_airport) AS unique_arrival_airports_count,
       ARRAY_AGG(arrival_airport ORDER BY arrival_airport) AS all_arrival_airports
FROM bookings.flights
GROUP BY departure_airport;

