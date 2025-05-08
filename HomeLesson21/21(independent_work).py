from utils.scripts import get_connection
import pandas as pd


"""Задания для самостоятельной работы:
1. Создать функцию, которая экспортирует информации о пассажирах конкретного рейса"""

def info_passenger(file_path: str, conn, flight: str):
    with get_connection() as conn: 

        query = f"""
            SELECT 
                tf.flight_id,
                t.passenger_id,
                t.passenger_name,
                t.contact_data        
            FROM bookings.ticket_flights tf
            join bookings.tickets t ON t.ticket_no = tf.ticket_no
            JOIN bookings.flights f ON f.flight_id = tf.flight_id
            WHERE flight_no = '{flight}';
        """

        df = pd.read_sql(query, conn)
        df.to_csv(file_path, index=False)
        print('данные экспортированы')
        conn.commit()

        print('файл добавлен')



"""2. Создать функцию, добавление билетов из массива в таблицу tickets
('1234567890123', 'ABCDEF', 'PASS1', 'Иванов Иван'),
('1234567890124', 'ABCDEF', 'PASS2', 'Петров Петр')"""

def add_tickets(ticket_no:str, book_ref:str, passenger_id:str, passenger_name:str) -> None:
    with get_connection() as conn: 
        with conn.cursor() as cursor:  

            query = """
                INSERT into bookings.tickets (ticket_no, book_ref, passenger_id, passenger_name)
                VALUES (%s, %s, %s, %s);
            """

            cursor.execute(query, (ticket_no, book_ref, passenger_id, passenger_name))
            conn.commit()

            print('Новая запись добавлена')



"""Создать функцию, увеличения цены на билеты для определенного класса обслуживания на определённый процент"""

def update_tickets(percent:int, fare_conditions:str) -> None:
    with get_connection() as conn:
        with conn.cursor() as cursor:
            
            query = """
                UPDATE bookings.ticket_flights
                SET amount = amount * (1 + %s/100.0)
                WHERE fare_conditions = %s;
            """
            cursor.execute(query, (percent, fare_conditions))
            
            conn.commit()
            print("Сделано")


if __name__ == '__main__':
    # info_passenger('passenger.csv', get_connection(), 'PG0014')
    # add_tickets('1234567890123', '000010', 'PASS1', 'Иванов Иван')
    update_tickets(50, 'Business')
