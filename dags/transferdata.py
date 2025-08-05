from airflow import DAG # type: ignore
from datetime import timedelta, datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.operators.python import PythonOperator # type: ignore

def transfer_data():
    mssql = MsSqlHook(mssql_conn_id="mssql_conn")
    postgres = PostgresHook(postgres_conn_id="postgres_conn")

    src_sql = "SELECT sCode as broker_id , sBrokerGroup as broker_name, sBrokerGroupSearch as describe FROM BrokerGroup"

    rows = mssql.get_records(src_sql)
    clean_rows = [
      tuple(str(col) if not isinstance(col, str) else col for col in row)
      for row in rows
    ]

    postgres.run("TRUNCATE TABLE brokers")
    insert_sql = """
      INSERT INTO brokers (broker_id, broker_name, describe)
      VALUES (%s, %s, %s)
    """
    for row in clean_rows:
      postgres.run(insert_sql, parameters=row)
    
    # postgres.insert_rows(
    #    table="brokers",
    #    rows=clean_rows,
    #    target_fields=["broker_id", "broker_name", "describe"]
    #   )

with DAG(
  dag_id="transfer_data_mssql_to_postgres",
  description="This is a dag for sales pipline.",
  start_date=datetime(2025,8,4,11,20),
  schedule_interval="*/5 * * * *",
  catchup=False,
  dagrun_timeout=timedelta(minutes=45),
  tags=["sales", "daily"]
) as dag:

  move_data=PythonOperator(
    task_id="transfer_data",
    python_callable=transfer_data
  )
