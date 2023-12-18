from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum

import os

import include.qa.SnowflakeQA_DropEndedEngagementDBs.local_sql as local_sql

default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

base_dir = os.path.dirname(os.path.realpath(__file__))
include = os.path.join(base_dir, 'include')
sql_dir = os.path.join(include, 'sql')

snowflake_name = 'Azure_West_Snowflake_QA'

def get_dbs_to_drop():
    sf_hook = SnowflakeHook(snowflake_conn_id=snowflake_name)
    con = sf_hook.get_conn()
    cur = con.cursor()
    cur.execute(local_sql.select_dbs_to_drop)
    recs = cur.fetchall()
    recs_formatted = []
    for rec in recs:
        recs_formatted.append({'db_name': rec[0]})
    print(recs_formatted)

    return recs_formatted

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    start_date=pendulum.datetime(2023, 12, 12, tz="America/New_York"),
    default_args=default_args,
    description=f"Drops ended V4 engagement dbs from QA Snowflake",
    catchup=False,
    template_searchpath = [sql_dir],
    schedule_interval='0 0 * * *'
) as dag:

    get_dbs_to_drop = PythonOperator(
        task_id="get_dbs_to_drop",
        python_callable=get_dbs_to_drop,
        provide_context=True
    )

    drop_dbs = SQLExecuteQueryOperator.partial(
        task_id=f"drop_dbs",
        conn_id=snowflake_name,
        autocommit=True,
        sql=local_sql.drop_stmnt
    ).expand(params=get_dbs_to_drop.output)

    get_dbs_to_drop >> drop_dbs