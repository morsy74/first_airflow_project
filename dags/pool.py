import time
from airflow import DAG # type: ignore
from datetime import timedelta, datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.sensors.sql import SqlSensor # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.models import Variable


def processing_data() :
  time.sleep(10)

with DAG(
  dag_id="pools_dag",
  description="This is a dag for sales pipline.",
  start_date=datetime(2025,8,4,13,34),
  schedule_interval="*/5 * * * *",
  catchup=False,
  dagrun_timeout=timedelta(minutes=45),
  tags=["sales", "daily"]
) as dag:
  create_table=PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres_conn",
    sql='''
      CREATE TABLE IF NOT EXISTS customers (
        customer_id VARCHAR(50) NOT NULL,
        customer_name VARCHAR NOT NULL,
        address VARCHAR NOT NULL,
        birth_date DATE NOT NULL
      )
    '''
  )

  transform_data_1=PythonOperator(
    task_id="transform_data_1",
    python_callable=processing_data
  )

  transform_data_2=PythonOperator(
    task_id="transform_data_2",
    python_callable=processing_data
  )

  transform_data_3=PythonOperator(
    task_id="transform_data_3",
    python_callable=processing_data
  )

  transform_data_4=PythonOperator(
    task_id="transform_data_4",
    python_callable=processing_data
  )

  select_values=PostgresOperator(
    task_id="select_values",
    postgres_conn_id="postgres_conn",
    sql='''
      SELECT * FROM customers WHERE birth_date BETWEEN %(start_date)s AND %(end_date)s
    ''',
    parameters={"start_date": "{{var.value.start_date}}", "end_date": "{{var.value.end_date}}"}
  )

  create_table >> [transform_data_1,transform_data_2,transform_data_3,transform_data_4] >> select_values