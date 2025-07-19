from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from faker import Faker
import random
from neo4j import GraphDatabase
from datetime import datetime
import logging

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2025, 7, 13),
}


def generate_bank_data():
    fake = Faker()
    data = []

    for _ in range(100):
        record = {
            "transaction_id": fake.uuid4(),
            "account_number": fake.bban(),
            "customer_name": fake.name(),
            "transaction_type": random.choice(["deposit", "transfer", "payment"]),
            "amount": round(random.uniform(10, 10000), 2),
            "currency": random.choice(["USD", "EUR", "RUB"]),
            "transaction_date": fake.date_time_this_month(),
            "bank_name": fake.company(),
            "branch_code": fake.swift11(),
            "status": random.choice(["completed", "pending", "failed"]),
            "created_at": datetime.now().isoformat(),
        }

        data.append(record)

    return data


def insert_to_neo4j(**kwargs):
    data = kwargs["ti"].xcom_pull(task_ids="generate_data")

    # Подключение к базе данных Neo4j

    uri = Variable.get("neo4j_uri")
    username = Variable.get("neo4j_username")
    password = Variable.get("neo4j_password")

    driver = GraphDatabase.driver(uri, auth=(username, password))

    with driver.session() as session:
        for record in data:
            session.run(
                """
                CREATE (t:Transaction {
                    transaction_id: $transaction_id,
                    account_number: $account_number,
                    customer_name: $customer_name,
                    transaction_type: $transaction_type,
                    amount: $amount,
                    currency: $currency,
                    transaction_date: $transaction_date,
                    bank_name: $bank_name,
                    branch_code: $branch_code,
                    status: $status,
                    created_at: $created_at
                })
                """,
                transaction_id=record["transaction_id"],
                account_number=record["account_number"],
                customer_name=record["customer_name"],
                transaction_type=record["transaction_type"],
                amount=record["amount"],
                currency=record["currency"],
                transaction_date=record["transaction_date"],
                bank_name=record["bank_name"],
                branch_code=record["branch_code"],
                status=record["status"],
                created_at=record["created_at"],
            )

        logging.info(f"Inserted {len(data)} records into Neo4j")

    driver.close()


with DAG(
    "42_NEO4J_generate_bank_data",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:

    generate_data = PythonOperator(
        task_id="generate_data", python_callable=generate_bank_data
    )

    load_to_neo4j = PythonOperator(
        task_id="load_to_neo4j", python_callable=insert_to_neo4j
    )

    generate_data >> load_to_neo4j
