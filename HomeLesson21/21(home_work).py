import pandas as pd
import sys
from pathlib import Path
import psycopg2
import json



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
                WHERE aa.city = %s AND ad.city = %s
                GROUP BY f.flight_no, f.scheduled_arrival, f.scheduled_departure, ai.model;"""
            
            cursor.execute(query, (arrival_city, departure_city))
            rows = cursor.fetchall()
            # print(rows)

            # #print(cursor.description) информация о столбцах результата последнего sql запроса.

            column_names = [el[0] for el in cursor.description]
            
            df = pd.DataFrame(rows, columns=column_names)
            
            df.to_csv(filename, index=False)

            print(f'файл создан {filename}')


"""Задача 2. Массовое обновление статусов рейсов
Создать функцию для пакетного обновления статусов рейсов (например, "Задержан" или "Отменен"). Функция должна:
- Принимать список рейсов и их новых статусов
- Подтверждать количество обновленных записей
- Обрабатывать ошибки (например, несуществующие рейсы)"""


def update_status_flights(json_data: str) -> None:
    try:
        data = json.loads(json_data)
        
        # Извлекаем значения из JSON
        flight_no = data.get("flight_no")
        new_status = data.get("new_status")

        # Проверяем, что все необходимые данные присутствуют
        if not isinstance(flight_no, list) or not flight_no:
            raise ValueError("flight_no должен быть непустым списком.")
        if not isinstance(new_status, str) or not new_status.strip():
            raise ValueError("new_status должен быть непустой строкой.")
        
        with get_connection() as conn:
            with conn.cursor() as cursor:
        
                query = """
                    UPDATE bookings.flights
                    SET status = %s
                    WHERE flight_no = ANY (%s)
                """
                cursor.execute(query, (new_status, flight_no))
                conn.commit()

                print(f'Обновлено рейсов: {cursor.statusmessage}')

    except psycopg2.OperationalError as e:
        print(f'Ошибка подключения к базе данных: {e}')
    except psycopg2.ProgrammingError as e:
        print(f'Ошибка в SQL-запросе: {e}')  
    except Exception as e:
        print(f'Произошла ошибка: {e}')  


"""Задача 3. Динамическое ценообразование
Реализовать функцию, которая автоматически корректирует цены на билеты в зависимости от спроса:
- Повышает цены на 10%, если продано >80% мест
- Понижает на 5%, если продано <30% мест
- Не изменяет цены бизнес-класса"""

def dinamyc_ticket_update() -> None:
    with get_connection() as conn:
        with conn.cursor() as cursor:

            query = """
                WITH seat_stats AS (
                    SELECT 
                        tf.flight_id,
                        tf.fare_conditions,
                        ns.cnt_seat AS all_seats,
                        COUNT(tf.ticket_no) AS seats_sold,
                        (COUNT(tf.ticket_no) * 100.0 / ns.cnt_seat) AS sold_percentage
                    FROM bookings.ticket_flights tf
                    JOIN bookings.flights f ON f.flight_id = tf.flight_id
                    JOIN bookings.numb_seat ns ON ns.aircraft_code = f.aircraft_code
                    GROUP BY tf.flight_id, tf.fare_conditions, ns.cnt_seat
                )
                UPDATE bookings.ticket_flights
                SET amount = CASE
                    WHEN ss.fare_conditions = 'Business' THEN amount -- Цены бизнес-класса не изменяются
                    WHEN ss.sold_percentage > 80 THEN amount * 1.10 -- Повышение на 10%
                    WHEN ss.sold_percentage < 30 THEN amount * 0.95 -- Понижение на 5%
                    ELSE amount -- В остальных случаях цена не меняется
                END
                FROM seat_stats ss
                WHERE bookings.ticket_flights.flight_id = ss.flight_id
                AND bookings.ticket_flights.fare_conditions = ss.fare_conditions;
            """

            cursor.execute(query,)
            
            print('Данный обновлены')

if __name__ == '__main__':
    # shedule_flights('shedule_flights', 'Москва', 'Санкт-Петербург')
    update_status_flights('{"flight_no": ["PG0402", "PG0222"], "new_status": "Scheduled"}')

    # dinamyc_ticket_update()