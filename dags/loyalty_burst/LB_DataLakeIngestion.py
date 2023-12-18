from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum

import os
import sys

import include.loyalty_burst.LB_DataLakeIngestion.local_sql as local_sql

default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

base_dir = os.path.dirname(os.path.realpath(__file__))
include = os.path.join(base_dir, 'include')
sql_dir = os.path.join(include, 'sql')

snowflake_name = 'AWS_Snowflake'

def get_lb_source_tbls():
    sf_hook = SnowflakeHook(snowflake_conn_id=snowflake_name)
    con = sf_hook.get_conn()
    cur = con.cursor()
    cur.execute(local_sql.sources_query)
    recs = cur.fetchall()
    recs_formatted = []
    for rec in recs:
        recs_formatted.append({'db_name': rec[0], 'tbl_name': rec[1]})
    print(recs_formatted)
    return recs_formatted

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    start_date=pendulum.datetime(2023, 12, 12, tz="America/New_York"),
    default_args=default_args,
    description=f"Handles all Loyalty Burst stored procedures",
    catchup=False,
    template_searchpath = [sql_dir],
    schedule_interval='30 7-22 * * 1-5'
) as dag:

    get_lb_source_tables = PythonOperator(
        task_id="get_lb_source_tbls",
        python_callable=get_lb_source_tbls,
        provide_context=True
    )

    create_to_create_batch = SQLExecuteQueryOperator.partial(
        task_id=f"call_create_to_create_batch",
        conn_id=snowflake_name,
        autocommit=True,
        sql=local_sql.create_to_create_batch
    ).expand(params=get_lb_source_tables.output)

    create_batch_to_stg_create = SQLExecuteQueryOperator.partial(
        task_id=f"call_create_batch_to_stg_create",
        conn_id=snowflake_name,
        autocommit=True,
        sql=local_sql.create_batch_to_stg_create
    ).expand(params=get_lb_source_tables.output)

    stg_create_to_DL = SQLExecuteQueryOperator.partial(
        task_id=f"call_stg_create_to_DL",
        conn_id=snowflake_name,
        autocommit=True,
        sql=local_sql.stg_create_to_DL
    ).expand(params=get_lb_source_tables.output)

    del_from_raw_and_create = SQLExecuteQueryOperator.partial(
        task_id=f"call_del_from_raw_and_create",
        conn_id=snowflake_name,
        autocommit=True,
        sql=local_sql.del_from_raw_and_create
    ).expand(params=get_lb_source_tables.output)

    get_lb_source_tables >> create_to_create_batch >> create_batch_to_stg_create >> stg_create_to_DL >> del_from_raw_and_create