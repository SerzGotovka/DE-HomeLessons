Задача 2:
1. Проанализировать производительность запроса без индексов.
2. Добавить индексы для ускорения JOIN и фильтрации.
3. Снова проанализировать производительность запроса и сравнить результаты.


EXPLAIN ANALYZE 
SELECT bp.boarding_no, t.passenger_id
FROM bookings.boarding_passes bp
JOIN bookings.tickets t ON bp.ticket_no = t.ticket_no
JOIN bookings.seats s ON bp.seat_no = s.seat_no
JOIN bookings.bookings b ON t.book_ref = b.book_ref
WHERE 
  t.passenger_id in ('0856 579180', '4723 695013')
  and boarding_no < 100
;

-- Результат до индексов
Gather  (cost=1001.26..65853.72 rows=20 width=16) (actual time=159.718..211.189 rows=65 loops=1)
  Workers Planned: 2
  Workers Launched: 2
  ->  Nested Loop  (cost=1.27..64851.72 rows=8 width=16) (actual time=126.976..167.139 rows=22 loops=3)
        ->  Nested Loop  (cost=0.99..64830.96 rows=2 width=19) (actual time=126.887..166.946 rows=3 loops=3)
              ->  Nested Loop  (cost=0.43..64810.29 rows=1 width=26) (actual time=126.860..166.915 rows=1 loops=3)
                    ->  Parallel Seq Scan on tickets t  (cost=0.00..64803.84 rows=1 width=33) (actual time=126.738..166.791 rows=1 loops=3)
                          Filter: ((passenger_id)::text = ANY ('{"0856 579180","4723 695013"}'::text[]))
                          Rows Removed by Filter: 983285
                    ->  Index Only Scan using bookings_pkey on bookings b  (cost=0.43..6.45 rows=1 width=7) (actual time=0.176..0.176 rows=1 loops=2)
                          Index Cond: (book_ref = t.book_ref)
                          Heap Fetches: 0
              ->  Index Scan using boarding_passes_pkey on boarding_passes bp  (cost=0.56..20.64 rows=3 width=21) (actual time=0.037..0.042 rows=5 loops=2)
                    Index Cond: (ticket_no = t.ticket_no)
                    Filter: (boarding_no < 100)
        ->  Index Only Scan using seats_pkey on seats s  (cost=0.28..10.35 rows=3 width=3) (actual time=0.033..0.056 rows=6 loops=10)
              Index Cond: (seat_no = (bp.seat_no)::text)
              Heap Fetches: 0
Planning Time: 0.636 ms
Execution Time: 211.548 ms


-- применение индекса на bookings.boarding_passes.ticket_no (размер 140 MB)
CREATE INDEX idx_boarding_passes_ticket_no
ON bookings.boarding_passes (ticket_no);


--После применения индекса на bookings.boarding_passes.ticket_no
Gather  (cost=1001.14..65853.59 rows=20 width=16) (actual time=1339.470..1851.694 rows=65 loops=1)
  Workers Planned: 2
  Workers Launched: 2
  ->  Nested Loop  (cost=1.14..64851.59 rows=8 width=16) (actual time=1155.527..1812.703 rows=22 loops=3)
        ->  Nested Loop  (cost=0.86..64830.83 rows=2 width=19) (actual time=1155.278..1812.240 rows=3 loops=3)
              ->  Nested Loop  (cost=0.43..64810.29 rows=1 width=26) (actual time=1153.895..1810.830 rows=1 loops=3)
                    ->  Parallel Seq Scan on tickets t  (cost=0.00..64803.84 rows=1 width=33) (actual time=1153.424..1810.321 rows=1 loops=3)
                          Filter: ((passenger_id)::text = ANY ('{"0856 579180","4723 695013"}'::text[]))
                          Rows Removed by Filter: 983285
                    ->  Index Only Scan using bookings_pkey on bookings b  (cost=0.43..6.45 rows=1 width=7) (actual time=0.583..0.583 rows=1 loops=2)
                          Index Cond: (book_ref = t.book_ref)
                          Heap Fetches: 0
              ->  Index Scan using idx_boarding_passes_ticket_no on boarding_passes bp  (cost=0.43..20.51 rows=3 width=21) (actual time=1.983..1.992 rows=5 loops=2)
                    Index Cond: (ticket_no = t.ticket_no)
                    Filter: (boarding_no < 100)
        ->  Index Only Scan using seats_pkey on seats s  (cost=0.28..10.35 rows=3 width=3) (actual time=0.064..0.100 rows=6 loops=10)
              Index Cond: (seat_no = (bp.seat_no)::text)
              Heap Fetches: 0
Planning Time: 16.637 ms
Execution Time: 1853.127 ms


-- применение индекса на bookings.seats.seat_no (размер 40 KB)
CREATE INDEX idx_seats_seat_no
ON bookings.seats (seat_no);


--После применения индекса на bookings.seats.seat_no
Nested Loop  (cost=1001.14..65832.67 rows=20 width=16) (actual time=2799.651..2802.079 rows=65 loops=1)
  ->  Gather  (cost=1000.86..65831.23 rows=4 width=19) (actual time=2799.625..2800.480 rows=10 loops=1)
        Workers Planned: 2
        Workers Launched: 2
        ->  Nested Loop  (cost=0.86..64830.83 rows=2 width=19) (actual time=2110.585..2788.147 rows=3 loops=3)
              ->  Nested Loop  (cost=0.43..64810.29 rows=1 width=26) (actual time=2110.528..2787.982 rows=1 loops=3)
                    ->  Parallel Seq Scan on tickets t  (cost=0.00..64803.84 rows=1 width=33) (actual time=2110.198..2787.626 rows=1 loops=3)
                          Filter: ((passenger_id)::text = ANY ('{"0856 579180","4723 695013"}'::text[]))
                          Rows Removed by Filter: 983285
                    ->  Index Only Scan using bookings_pkey on bookings b  (cost=0.43..6.45 rows=1 width=7) (actual time=0.524..0.524 rows=1 loops=2)
                          Index Cond: (book_ref = t.book_ref)
                          Heap Fetches: 0
              ->  Index Scan using idx_boarding_passes_ticket_no on boarding_passes bp  (cost=0.43..20.51 rows=3 width=21) (actual time=0.139..0.242 rows=5 loops=2)
                    Index Cond: (ticket_no = t.ticket_no)
                    Filter: (boarding_no < 100)
  ->  Index Only Scan using idx_seats_seat_no on seats s  (cost=0.28..0.33 rows=3 width=3) (actual time=0.158..0.158 rows=6 loops=10)
        Index Cond: (seat_no = (bp.seat_no)::text)
        Heap Fetches: 0
Planning Time: 3.021 ms
Execution Time: 2802.163 ms


-- применение индекса на bookings.bookings.book_ref (размер 45 MB)
CREATE INDEX idx_bookings_book_ref
ON bookings.bookings (book_ref);


--После применения индекса на bookings.bookings.book_ref
Nested Loop  (cost=1001.14..65832.67 rows=20 width=16) (actual time=1102.442..1460.090 rows=65 loops=1)
  ->  Gather  (cost=1000.86..65831.23 rows=4 width=19) (actual time=1102.433..1459.684 rows=10 loops=1)
        Workers Planned: 2
        Workers Launched: 2
        ->  Nested Loop  (cost=0.86..64830.83 rows=2 width=19) (actual time=1104.489..1444.175 rows=3 loops=3)
              ->  Nested Loop  (cost=0.43..64810.29 rows=1 width=26) (actual time=1104.456..1444.137 rows=1 loops=3)
                    ->  Parallel Seq Scan on tickets t  (cost=0.00..64803.84 rows=1 width=33) (actual time=1103.837..1443.517 rows=1 loops=3)
                          Filter: ((passenger_id)::text = ANY ('{"0856 579180","4723 695013"}'::text[]))
                          Rows Removed by Filter: 983285
                    ->  Index Only Scan using idx_bookings_book_ref on bookings b  (cost=0.43..6.45 rows=1 width=7) (actual time=0.922..0.923 rows=1 loops=2)
                          Index Cond: (book_ref = t.book_ref)
                          Heap Fetches: 0
              ->  Index Scan using idx_boarding_passes_ticket_no on boarding_passes bp  (cost=0.43..20.51 rows=3 width=21) (actual time=0.045..0.051 rows=5 loops=2)
                    Index Cond: (ticket_no = t.ticket_no)
                    Filter: (boarding_no < 100)
  ->  Index Only Scan using idx_seats_seat_no on seats s  (cost=0.28..0.33 rows=3 width=3) (actual time=0.038..0.039 rows=6 loops=10)
        Index Cond: (seat_no = (bp.seat_no)::text)
        Heap Fetches: 0
Planning Time: 55.901 ms
Execution Time: 1460.216 ms


-- применение индекса на bookings.tickets.ticket_no (размер 88 MB)
CREATE INDEX idx_bookings_ticket_no
ON bookings.tickets (ticket_no);



--После применения индекса на bookings.tickets.ticket_no
Nested Loop  (cost=1001.14..65832.67 rows=20 width=16) (actual time=86.671..124.619 rows=65 loops=1)
  ->  Gather  (cost=1000.86..65831.23 rows=4 width=19) (actual time=86.663..124.491 rows=10 loops=1)
        Workers Planned: 2
        Workers Launched: 2
        ->  Nested Loop  (cost=0.86..64830.83 rows=2 width=19) (actual time=74.596..111.335 rows=3 loops=3)
              ->  Nested Loop  (cost=0.43..64810.29 rows=1 width=26) (actual time=74.510..111.188 rows=1 loops=3)
                    ->  Parallel Seq Scan on tickets t  (cost=0.00..64803.84 rows=1 width=33) (actual time=74.320..110.996 rows=1 loops=3)
                          Filter: ((passenger_id)::text = ANY ('{"0856 579180","4723 695013"}'::text[]))
                          Rows Removed by Filter: 983285
                    ->  Index Only Scan using idx_bookings_book_ref on bookings b  (cost=0.43..6.45 rows=1 width=7) (actual time=0.278..0.278 rows=1 loops=2)
                          Index Cond: (book_ref = t.book_ref)
                          Heap Fetches: 0
              ->  Index Scan using idx_boarding_passes_ticket_no on boarding_passes bp  (cost=0.43..20.51 rows=3 width=21) (actual time=0.126..0.214 rows=5 loops=2)
                    Index Cond: (ticket_no = t.ticket_no)
                    Filter: (boarding_no < 100)
  ->  Index Only Scan using idx_seats_seat_no on seats s  (cost=0.28..0.33 rows=3 width=3) (actual time=0.011..0.011 rows=6 loops=10)
        Index Cond: (seat_no = (bp.seat_no)::text)
        Heap Fetches: 0
Planning Time: 6.880 ms
Execution Time: 125.184 ms



-- применение индекса на bookings.tickets.passenger_id (размер 88 MB)
CREATE INDEX idx_tickets_passenger_id
ON bookings.tickets (passenger_id);


--После применения индекса на bookings.tickets.passenger_id
Nested Loop  (cost=1.57..72.31 rows=20 width=16) (actual time=3.547..5.856 rows=65 loops=1)
  ->  Nested Loop  (cost=1.29..70.87 rows=4 width=19) (actual time=3.542..5.794 rows=10 loops=1)
        ->  Nested Loop  (cost=0.86..29.79 rows=2 width=26) (actual time=3.468..5.474 rows=2 loops=1)
              ->  Index Scan using idx_tickets_passenger_id on tickets t  (cost=0.43..16.89 rows=2 width=33) (actual time=3.382..5.082 rows=2 loops=1)
                    Index Cond: ((passenger_id)::text = ANY ('{"0856 579180","4723 695013"}'::text[]))
              ->  Index Only Scan using idx_bookings_book_ref on bookings b  (cost=0.43..6.45 rows=1 width=7) (actual time=0.193..0.193 rows=1 loops=2)
                    Index Cond: (book_ref = t.book_ref)
                    Heap Fetches: 0
        ->  Index Scan using idx_boarding_passes_ticket_no on boarding_passes bp  (cost=0.43..20.51 rows=3 width=21) (actual time=0.081..0.158 rows=5 loops=2)
              Index Cond: (ticket_no = t.ticket_no)
              Filter: (boarding_no < 100)
  ->  Index Only Scan using idx_seats_seat_no on seats s  (cost=0.28..0.33 rows=3 width=3) (actual time=0.005..0.005 rows=6 loops=10)
        Index Cond: (seat_no = (bp.seat_no)::text)
        Heap Fetches: 0
Planning Time: 11.937 ms
Execution Time: 6.712 ms


-- применение индекса на bookings.boarding_passes.boarding_no (размер 52 MB)
CREATE INDEX idx_boarding_passes_boarding_no
ON bookings.boarding_passes (boarding_no);


--После применения индекса на bookings.boarding_passes.boarding_no
Nested Loop  (cost=1.57..72.31 rows=20 width=16) (actual time=3.232..8.758 rows=65 loops=1)
  ->  Nested Loop  (cost=1.29..70.87 rows=4 width=19) (actual time=3.158..8.275 rows=10 loops=1)
        ->  Nested Loop  (cost=0.86..29.79 rows=2 width=26) (actual time=2.105..4.620 rows=2 loops=1)
              ->  Index Scan using idx_tickets_passenger_id on tickets t  (cost=0.43..16.89 rows=2 width=33) (actual time=1.595..3.362 rows=2 loops=1)
                    Index Cond: ((passenger_id)::text = ANY ('{"0856 579180","4723 695013"}'::text[]))
              ->  Index Only Scan using idx_bookings_book_ref on bookings b  (cost=0.43..6.45 rows=1 width=7) (actual time=0.625..0.625 rows=1 loops=2)
                    Index Cond: (book_ref = t.book_ref)
                    Heap Fetches: 0
        ->  Index Scan using idx_boarding_passes_ticket_no on boarding_passes bp  (cost=0.43..20.51 rows=3 width=21) (actual time=0.814..1.824 rows=5 loops=2)
              Index Cond: (ticket_no = t.ticket_no)
              Filter: (boarding_no < 100)
  ->  Index Only Scan using idx_seats_seat_no on seats s  (cost=0.28..0.33 rows=3 width=3) (actual time=0.046..0.047 rows=6 loops=10)
        Index Cond: (seat_no = (bp.seat_no)::text)
        Heap Fetches: 0
Planning Time: 29.348 ms
Execution Time: 10.092 ms



"""
Время запроса уменьшилось в 20,9 раз

"""








