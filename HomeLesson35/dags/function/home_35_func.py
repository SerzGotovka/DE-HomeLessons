from airflow.providers.postgres.hooks.postgres import PostgresHook


pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
conn = pg_hook.get_conn()
cursor = conn.cursor()

def read_raw_users_orders_transform_and_load_data_mart():
  
    query = """SELECT u.user_name, u.user_surname, u.card_number, (SUM(o.total_amount)) AS sum_amount 
        FROM raw.users u
        JOIN raw.orders o 
        ON u.card_number = o.card_number
        GROUP BY u.user_name, u.card_number, u.user_surname;
        """

    cursor.execute(query)
    rows = cursor.fetchall()

    query_create_table = """
    CREATE TABLE IF NOT EXISTS data_mart.user_order_summary (
                    id serial primary key,
                    user_name TEXT,
                    user_sername TEXT,
                    card_number TEXT,
                    total_amount NUMERIC
                );
            """

    cursor.execute(query_create_table)
    conn.commit()
    print('[V] Таблица user_order_summary создана')


    for row in rows:
        name, surname, card_number, total_amount = row[0], row[1], row[2], row[3]

        cursor.execute(
            """
                INSERT INTO data_mart.user_order_summary (user_name, user_sername, card_number, total_amount)
                VALUES (%s, %s, %s, %s)
                -- on conflict (user_name, user_sername, card_number, total_amount) DO NOTHING;
                """, (
                    name,
                    surname,
                    card_number,
                    total_amount)
        )
        print('[V] Данные в user_order_summary загружены')
        conn.commit()

 
    # print(res)
    conn.close()
    cursor.close()
 


