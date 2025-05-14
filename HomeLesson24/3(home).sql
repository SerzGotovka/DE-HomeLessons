GroupAggregate — это тип операции в плане выполнения SQL-запроса, который используется для группировки строк и применения агрегатных функций (например, SUM, COUNT, AVG, MIN, MAX и т.д.) по группам.

Он отличается от другого похожего типа операции — HashAggregate , тем что GroupAggregate требует предварительной сортировки данных по ключу группировки, чтобы объединять одинаковые значения.


Когда используется GroupAggregate?
GroupAggregate чаще всего используется, когда:

Нужно выполнить GROUP BY по нескольким столбцам.
Таблица уже отсортирована по группировочным полям (например, есть индекс).
Количество уникальных групп велико, и использование хэш-таблицы (HashAggregate) неэффективно.

-- Здесь явно и сразу используется GroupAggregate
EXPLAIN analyze
SELECT flight_no, count(*) FROM bookings.flights
GROUP BY flight_no


Finalize GroupAggregate  (cost=5662.11..5754.41 rows=710 width=15) (actual time=38.002..40.693 rows=710 loops=1)
  Group Key: flight_no
  ->  Gather Merge  (cost=5662.11..5743.76 rows=710 width=15) (actual time=37.996..40.545 rows=1038 loops=1)
        Workers Planned: 1
        Workers Launched: 1
        ->  Sort  (cost=4662.10..4663.87 rows=710 width=15) (actual time=19.352..19.370 rows=519 loops=2)
              Sort Key: flight_no
              Sort Method: quicksort  Memory: 52kB
              Worker 0:  Sort Method: quicksort  Memory: 38kB
              ->  Partial HashAggregate  (cost=4621.37..4628.47 rows=710 width=15) (actual time=18.729..18.784 rows=519 loops=2)
                    Group Key: flight_no
                    Batches: 1  Memory Usage: 105kB
                    Worker 0:  Batches: 1  Memory Usage: 73kB
                    ->  Parallel Seq Scan on flights  (cost=0.00..3988.25 rows=126625 width=7) (actual time=0.010..5.397 rows=107434 loops=2)
Planning Time: 0.074 ms
Execution Time: 40.813 ms



-- применяется HashAggregate
EXPLAIN analyze
SELECT book_ref, count(*) FROM bookings.tickets t 
GROUP BY book_ref


HashAggregate  (cost=244868.09..281604.83 rows=1369097 width=15) (actual time=2715.364..3529.633 rows=2111110 loops=1)
  Group Key: book_ref
  Planned Partitions: 16  Batches: 81  Memory Usage: 8337kB  Disk Usage: 80512kB
  ->  Seq Scan on tickets t  (cost=0.00..78938.58 rows=2949858 width=7) (actual time=2.570..1369.082 rows=2949858 loops=1)
Planning Time: 4.083 ms
JIT:
  Functions: 7
  Options: Inlining false, Optimization false, Expressions true, Deforming true
  Timing: Generation 3.849 ms, Inlining 0.000 ms, Optimization 11.620 ms, Emission 106.462 ms, Total 121.932 ms
Execution Time: 4548.750 ms


-- после добавления индекса на ключ группировки (сортировка не давала GroupAggregate):
CREATE INDEX idx_book_ref ON bookings.tickets (book_ref)


GroupAggregate  (cost=0.43..102776.56 rows=1369097 width=15) (actual time=1.433..687.236 rows=2111110 loops=1)
  Group Key: book_ref
  ->  Index Only Scan using idx_book_ref on tickets t  (cost=0.43..74336.30 rows=2949858 width=7) (actual time=0.018..264.771 rows=2949858 loops=1)
        Heap Fetches: 119
Planning Time: 0.065 ms
JIT:
  Functions: 3
  Options: Inlining false, Optimization false, Expressions true, Deforming true
  Timing: Generation 0.146 ms, Inlining 0.000 ms, Optimization 0.106 ms, Emission 1.300 ms, Total 1.552 ms
Execution Time: 743.819 ms