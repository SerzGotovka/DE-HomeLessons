from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import pandas as pd
from airflow.sensors.external_task import ExternalTaskSensor


with DAG(
    'agg_sessions',
    description = 'Расчет статистики',
    schedule_interval = '@daily',
    start_date = datetime(2025, 7, 7),
    catchup=False,
    tags = ['raw'],
) as dag:
    
    wait_for_load_task = ExternalTaskSensor(
        task_id = 'wait_for_load',
        external_dag_id = 'extract_and_prepare_data',
        external_task_id = 'joined_data_task',
        execution_date_fn = lambda dt: dt, #Этот параметр задает функцию, которая будет использоваться для вычисления даты выполнения задачи. В данном случае мы используем простую лямбда-функцию, которая возвращает ту же дату dt, которая передаётся в неё. Это означает, что для проверки завершенности задачи будет использована та же дата выполнения, что и у текущего DAGS.
        mode = 'reschedule', #Этот параметр определяет режим работы сенсора. Значение reschedule указывает, что Airflow будет периодически "просматривать" состояние задачи без блокировки текущего выполнения, освобождая ресурсы системы. Это позволяет Airflow более эффективно обрабатывать другие задачи, пока ожидается завершение внешней задачи.
        poke_interval = 30,
        timeout = 3600,
        allowed_states = ['success'], #Список состояний, которые считаются допустимыми для внешней задачи. В данном случае задается только одно состояние - success. Это значит, что сенсор будет ожидать, пока внешняя задача joined_data_task не завершится успешно.
        failed_states = ['failed', 'skipped'] #Этот параметр указывает состояние задач, которые считаются неудачными. В данном случае, если внешняя задача завершится со статусами failed или skipped, сенсор также будет считать задачу wait_for_load_task неуспешной.

    )

    aggregate_task = PostgresOperator(
        task_id = 'aggregate_task',
        postgres_conn_id = 'my_postgres_conn',
        sql = """
            insert into data_mart.session_summary(avg_duration_sec, avg_event_count)
            select 
            avg(duration_sec) as avg_duration,
            avg(event_count) as avg_count
            from temp.session_event_stats;
        """
    )

    wait_for_load_task >> aggregate_task