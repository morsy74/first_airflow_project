from airflow import DAG # type: ignore
from datetime import timedelta, datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore

with DAG(
  dag_id="myfirstdag",
  description="This is a dag for sales pipline.",
  start_date=datetime(2025,8,4,9,35),
  schedule_interval="*/5 * * * *",
  catchup=False,
  dagrun_timeout=timedelta(minutes=45),
  tags=["sales", "daily"]
) as dag:
  create_table=PostgresOperator(
    task_id="customer_table",
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

  # insert_values=PostgresOperator(
  #   task_id="insert_customers",
  #   postgres_conn_id="postgres_conn",
  #   sql='/sql/customers.sql'
  # )

  # create_table >> insert_values