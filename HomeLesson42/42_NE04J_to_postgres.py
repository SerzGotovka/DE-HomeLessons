import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from neo4j import GraphDatabase
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
import logging

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 13),
}


def transfer_from_neo4j_to_postgres():
    uri = Variable.get("neo4j_uri")
    username = Variable.get("neo4j_username")
    password = Variable.get("neo4j_password")

    driver = GraphDatabase.driver(uri, auth=(username, password))

    ten_min_ago = datetime.now(timezone.utc) - timedelta(minutes=1)
    formatted_time = ten_min_ago.isoformat()

    logging.info(
        f"Fetching data from Neo4j for records created after: {formatted_time}"
    )

    with driver.session() as session:
        query = """
        MATCH (t:Transaction)
        WHERE t.created_at >= $created_at
        RETURN t.transaction_id AS transaction_id,
               t.account_number AS account_number,
               t.customer_name AS customer_name,
               t.transaction_type AS transaction_type,
               t.amount AS amount,
               t.currency AS currency,
               t.transaction_date AS transaction_date,
               t.bank_name AS bank_name,
               t.branch_code AS branch_code,
               t.status AS status,
               t.created_at AS created_at
        """
        result = session.run(query, created_at=formatted_time)
        data = [record.data() for record in result]
        logging.info(f"Retrieved {len(data)} records from Neo4j")

    if data:
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        for record in data:
            # Преобразуем даты через pandas
            created_at = pd.to_datetime(str(record["created_at"])).to_pydatetime()
            transaction_date = pd.to_datetime(
                str(record["transaction_date"])
            ).to_pydatetime()

            cursor.execute(
                """
                INSERT INTO raw.bank_transactions_neo4j (
                    transaction_id, account_number, customer_name, transaction_type,
                    amount, currency, transaction_date, bank_name, branch_code, status, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
            """,
                (
                    record["transaction_id"],
                    record["account_number"],
                    record["customer_name"],
                    record["transaction_type"],
                    record["amount"],
                    record["currency"],
                    transaction_date,
                    record["bank_name"],
                    record["branch_code"],
                    record["status"],
                    created_at,
                ),
            )

        conn.commit()
        cursor.close()
        conn.close()

    driver.close()


with DAG(
    "42_NEO4J_integrate_data",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    transfer_from_neo4j_to_postgres_task = PythonOperator(
        task_id="transfer_from_neo4j_to_postgres",
        python_callable=transfer_from_neo4j_to_postgres,
    )
