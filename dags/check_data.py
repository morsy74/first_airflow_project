import requests
from airflow import DAG # type: ignore
from datetime import timedelta, datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.sensors.sql import SqlSensor # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

def processing_data() :
  print("records exists")

with DAG(
  dag_id="check_data",
  description="This is a dag for sales pipline.",
  start_date=datetime(2025,8,4,13,34),
  schedule_interval="*/5 * * * *",
  catchup=False,
  dagrun_timeout=timedelta(minutes=45),
  tags=["sales", "daily"]
) as dag:
  check_records=SqlSensor(
    task_id="check_records",
    conn_id="postgres_conn",
    sql=''' select * from brokers where broker_name = 'efwef'; ''',
    poke_interval=10,
    timeout=30,
    mode="reschedule"
  )

  processing_data=PythonOperator(
    task_id="processing_data",
    python_callable=processing_data
  )

  check_records >> processing_data

