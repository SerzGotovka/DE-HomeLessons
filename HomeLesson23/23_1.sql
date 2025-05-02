Задача 1:
Необходимо оптимизировать выборку данных по номеру места (bookings.boarding_passes.seat_no) 
с помощью индекса и сравнить результаты до добавления индекса и после (время выполнения и объем таблицы в %)


EXPLAIN ANALYZE 
SELECT * FROM bookings.boarding_passes
WHERE seat_no = '15A';

-- до приминения индексов 
Gather  (cost=1000.00..105709.67 rows=51254 width=25) (actual time=118.034..2748.837 rows=48552 loops=1)
  Workers Planned: 2
  Workers Launched: 2
  ->  Parallel Seq Scan on boarding_passes  (cost=0.00..99584.27 rows=21356 width=25) (actual time=48.103..2646.376 rows=16184 loops=3)
        Filter: ((seat_no)::text = '15A'::text)
        Rows Removed by Filter: 2625753
Planning Time: 0.339 ms
JIT:
  Functions: 6
  Options: Inlining false, Optimization false, Expressions true, Deforming true
  Timing: Generation 9.955 ms, Inlining 0.000 ms, Optimization 15.031 ms, Emission 123.838 ms, Total 148.824 ms
Execution Time: 3701.204 ms

-- применяем индекс на фильтрацию BTree(55 mb)

CREATE INDEX idx_boarding_passes_seat_no
ON bookings.boarding_passes (seat_no);


Bitmap Heap Scan on boarding_passes  (cost=573.65..60162.76 rows=51254 width=25) (actual time=19.938..9720.828 rows=48552 loops=1)
  Recheck Cond: ((seat_no)::text = '15A'::text)
  Heap Blocks: exact=37787
  ->  Bitmap Index Scan on idx_boarding_passes_seat_no  (cost=0.00..560.84 rows=51254 width=0) (actual time=15.170..15.171 rows=48552 loops=1)
        Index Cond: ((seat_no)::text = '15A'::text)
Planning Time: 1.407 ms
Execution Time: 9729.749 ms


-- применяем индекс на фильтрацию Hash(374 mb)
CREATE INDEX idx_boarding_passes_seat_no
ON bookings.boarding_passes USING HASH (seat_no);


Bitmap Heap Scan on boarding_passes  (cost=1637.22..61226.33 rows=51254 width=25) (actual time=10.319..2808.184 rows=48552 loops=1)
  Recheck Cond: ((seat_no)::text = '15A'::text)
  Heap Blocks: exact=37787
  ->  Bitmap Index Scan on idx_boarding_passes_seat_no  (cost=0.00..1624.40 rows=51254 width=0) (actual time=5.172..5.190 rows=48552 loops=1)
        Index Cond: ((seat_no)::text = '15A'::text)
Planning Time: 11.325 ms
Execution Time: 2813.724 ms




"""
размер таблицы  bookings.boarding_passes 1.1 GB (до индексов)

После применения индекса BTree:
    - Время запроса: 3701ms -> 9729ms = время увеличилось в 2,62 раза - (162%)
    - Размер таблицы: увеличился на 50 Mb - (4,55 %)

После применения индекса BTree:
    - Время запроса: 3701ms -> 2813ms = время уменьшилось в 1,31 раза - (24%)
    - Размер таблицы: увеличился на 300 Mb - (27%) 
"""