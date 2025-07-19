from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import pandas as pd
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def extract_session_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    data = pd.read_sql("select * from raw.sessions", engine)
    print(data)
    kwargs["ti"].xcom_push(key="sessions_data", value=data)


def extract_events_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    engine = pg_hook.get_sqlalchemy_engine()

    data = pd.read_sql("select * from raw.events", engine)
    print(data)
    kwargs["ti"].xcom_push(key="events_data", value=data)


def merge_data(**kwargs):
    sessions = kwargs["ti"].xcom_pull(
        key="sessions_data", task_ids="data_extraction.extract_sessions_data"
    )
    events = kwargs["ti"].xcom_pull(
        key="events_data", task_ids="data_extraction.extract_events_data"
    )

    print(sessions)
    print(events)

    sessions = sessions.values.tolist()
    events = events.values.tolist()

    session_dict = {}
    for sess in sessions:
        session_id = sess[0]
        session_dict[session_id] = {
            "duration_sec": sess[5],
            "event_count": 0,
            "session_date": sess[2],
        }

    for event in events:
        session_id = event[1]
        if session_id in session_dict:
            session_dict[session_id]["event_count"] += 1

    result = [
        (sess_id, data["duration_sec"], data["event_count"])
        for sess_id, data in session_dict.items()
    ]

    print(result)
    kwargs["ti"].xcom_push(key="joined_data", value=result)


def transform_data_check_null_duplicates(**kwargs):
    """функция для проверки данных session на наличие 0-значений"""
    ti = kwargs["ti"]
    data = ti.xcom_pull(key="joined_data", task_ids="merge_sessions_data")

    data_df = pd.DataFrame(data)

    # Находим индексы строк, которые нужно удалить
    indexes_to_drop = data_df[data_df.isin([0]).any(axis=1)].index
    # Удаляем строки по найденным индексам
    data_df.drop(indexes_to_drop)

    data_df.drop_duplicates()
    kwargs["ti"].xcom_push(key="df_clean_data", value=data_df)


def load_to_temp_table(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(
        key="df_clean_data", task_ids="transform_data_check_null_duplicates"
    )

    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    hook.run("DELETE FROM temp.session_event_stats", autocommit=True)

    for row in data:
        hook.run(
            f"""
            INSERT INTO temp.session_event_stats VALUES (
                '{row[0]}', '{row[1]}', '{row[2]}'
            )
        """,
            autocommit=True,
        )


with DAG(
    "40_extract_and_prepare_data",
    description="Извлечение данных",
    schedule_interval="@daily",
    start_date=datetime(2025, 7, 7),
    catchup=False,
    tags=["raw"],
) as dag:

    with TaskGroup("data_extraction") as extraction_group:

        extract_data_sessions_task = PythonOperator(
            task_id="extract_sessions_data", python_callable=extract_session_data
        )

        extract_data_events_task = PythonOperator(
            task_id="extract_events_data", python_callable=extract_events_data
        )

    extract_data_sessions_task >> extract_data_events_task

    merge_data_task = PythonOperator(
        task_id="merge_sessions_data", python_callable=merge_data
    )

    transform_data_check_null_duplicates_task = PythonOperator(
        task_id="transform_data_check_null_duplicates",
        python_callable=transform_data_check_null_duplicates,
        provide_context=True,
    )

    joined_data_task = PythonOperator(
        task_id="joined_data_task",
        python_callable=load_to_temp_table,
        provide_context=True,
    )

    trigger_dag = TriggerDagRunOperator(
        task_id="trigger_DAG2",
        trigger_dag_id="agg_sessions",
        execution_date="{{execution_date}}",  # Этот параметр задает дату и время, когда будет запущен целевой DAGS.
        wait_for_completion=False,  # Этот параметр определяет, должен ли текущий DAGS ждать завершения запущенного DAGS. Если значение False, то текущий DAGS продолжит выполнение, не дожидаясь завершения запущенного DAGS. В противном случае, если установлено значение True, текущий DAGS будет ожидать завершения работы второго DAGS перед продолжением.
        reset_dag_run=True,  # Это параметр, который указывает, следует ли сбрасывать запущенный DAGS, если он уже существует. Если вы установите это в True, во время следующего запуска будет сброшен статус запущенного DAGS, что может быть полезно, если вы хотите, чтобы каждый запуск DAGS начинался "с чистого листа".
        trigger_rule="all_success",  # Этот параметр определяет правило запуска задачи. В данном случае all_success означает, что задача trigger_dag    будет выполнена только в том случае, если все предыдущие задачи в текущем DAGS были успешно выполнены. Если хотя бы одна задача завершится неуспешно, задача не будет выполнена.
    )

    (
        extraction_group
        >> merge_data_task
        >> transform_data_check_null_duplicates_task
        >> joined_data_task
        >> trigger_dag
    )
