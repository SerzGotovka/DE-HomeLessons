Задача 1: Анализ частоты полетов пассажиров
Определить топ-10 пассажиров, которые чаще всего летают.
Провести оптимизацию скрипта по необходимости

--основной запрос выполняется за 1,5 сек, с приминением индекса 0,5 сек.
-- размер таблицы после применения индекса увеличился на 3%.
-- По моему применение индекса нецелесообразно, 1.5 сек наверное норм)


EXPLAIN ANALYZE 
SELECT 
	passenger_name,
	count(ticket_no) AS count_ticket
FROM bookings.tickets
GROUP BY passenger_name
ORDER BY count_ticket  DESC
LIMIT 10;

-- до приминения индексов
Limit  (cost=71493.86..71493.88 rows=10 width=24) (actual time=1531.552..1531.841 rows=10 loops=1)
  ->  Sort  (cost=71493.86..71519.86 rows=10402 width=24) (actual time=1531.551..1531.838 rows=10 loops=1)
        Sort Key: (count(ticket_no)) DESC
        Sort Method: top-N heapsort  Memory: 26kB
        ->  Finalize HashAggregate  (cost=71165.05..71269.07 rows=10402 width=24) (actual time=1522.778..1528.198 rows=34324 loops=1)
              Group Key: passenger_name
              Batches: 1  Memory Usage: 4113kB
              ->  Gather  (cost=68876.61..71061.03 rows=20804 width=24) (actual time=1481.158..1492.347 rows=86729 loops=1)
                    Workers Planned: 2
                    Workers Launched: 2
                    ->  Partial HashAggregate  (cost=67876.61..67980.63 rows=10402 width=24) (actual time=1339.356..1345.233 rows=28910 loops=3)
                          Group Key: passenger_name
                          Batches: 1  Memory Usage: 3857kB
                          Worker 0:  Batches: 1  Memory Usage: 3857kB
                          Worker 1:  Batches: 1  Memory Usage: 3857kB
                          ->  Parallel Seq Scan on tickets  (cost=0.00..61731.07 rows=1229108 width=30) (actual time=0.214..1041.865 rows=983286 loops=3)
Planning Time: 0.359 ms
Execution Time: 1543.638 ms


CREATE INDEX idx_passenger_name 
ON bookings.tickets (passenger_name)

-- после приминения индексов
Limit  (cost=71493.86..71493.88 rows=10 width=24) (actual time=500.782..500.930 rows=10 loops=1)
  ->  Sort  (cost=71493.86..71519.86 rows=10402 width=24) (actual time=500.781..500.927 rows=10 loops=1)
        Sort Key: (count(ticket_no)) DESC
        Sort Method: top-N heapsort  Memory: 26kB
        ->  Finalize HashAggregate  (cost=71165.05..71269.07 rows=10402 width=24) (actual time=488.521..495.791 rows=34324 loops=1)
              Group Key: passenger_name
              Batches: 1  Memory Usage: 4113kB
              ->  Gather  (cost=68876.61..71061.03 rows=20804 width=24) (actual time=446.140..458.680 rows=86738 loops=1)
                    Workers Planned: 2
                    Workers Launched: 2
                    ->  Partial HashAggregate  (cost=67876.61..67980.63 rows=10402 width=24) (actual time=442.082..448.512 rows=28913 loops=3)
                          Group Key: passenger_name
                          Batches: 1  Memory Usage: 3857kB
                          Worker 0:  Batches: 1  Memory Usage: 3857kB
                          Worker 1:  Batches: 1  Memory Usage: 3857kB
                          ->  Parallel Seq Scan on tickets  (cost=0.00..61731.07 rows=1229108 width=30) (actual time=0.165..120.484 rows=983286 loops=3)
Planning Time: 6.289 ms
Execution Time: 501.348 ms