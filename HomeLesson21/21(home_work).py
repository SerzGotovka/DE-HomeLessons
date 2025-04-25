import pandas as pd
import sys
from pathlib import Path


root_dir = Path(__file__).parent.parent  
sys.path.append(str(root_dir))

from utils.scripts import get_connection

"""Задача 1. Экспорт расписания рейсов по конкретному маршруту.
Нужно создать функцию на Python, которая выгружает в CSV-файл расписание рейсов между двумя городами (например, Москва и Санкт-Петербург). Функция должна включать:
- Номер рейса
- Время вылета и прилета
- Тип самолета
- Среднюю цену билета
SELECT сделать без использования pandas!"""

def shedule_flights(filename:str, arrival_city:str, departure_city:str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cursor:     

            query = """
                SELECT 
                    f.flight_no,
                    to_char(f.scheduled_arrival, 'YYYY-MM-DD HH24:MI:SS') as scheduled_arrival,
                    to_char(f.scheduled_departure, 'YYYY-MM-DD HH24:MI:SS') as scheduled_departure,
                    ai.model,
                    ROUND(AVG(tf.amount), 2) AS avg_ticket_price
                FROM bookings.flights f
                JOIN bookings.airports aa ON f.arrival_airport = aa.airport_code
                JOIN bookings.airports ad ON f.departure_airport = ad.airport_code
                JOIN bookings.aircrafts ai ON f.aircraft_code  = ai.aircraft_code 
                JOIN bookings.ticket_flights tf ON tf.flight_id = f.flight_id
                WHERE aa.city = 'Москва' AND ad.city = 'Санкт-Петербург'
                GROUP BY f.flight_no, f.scheduled_arrival, f.scheduled_departure, ai.model;"""
            
            cursor.execute(query, (arrival_city, departure_city))
            rows = cursor.fetchall()
            # print(rows)

            # #print(cursor.description) информация о столбцах результата последнего sql запроса.

            column_names = [el[0] for el in cursor.description]
            
            df = pd.DataFrame(rows, columns=column_names)
            
            df.to_csv(filename, index=False)

            print(f'файл создан {filename}')


if __name__ == '__main__':
    shedule_flights('shedule_flights', 'Москва', 'Санкт-Петербург')