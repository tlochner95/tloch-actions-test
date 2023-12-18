from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

import time
import os

import include.sql.mailgun.mailgun_event_load as mailgun_event_load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

base_dir = os.path.dirname(os.path.realpath(__file__))
include = os.path.join(base_dir, 'include')
sql_dir = os.path.join(include, 'sql')

def sleep():
    print("Sleep for 2 minutes")
    time.sleep(180)

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    start_date=datetime(2023, 11, 29),
    default_args=default_args,
    description=f"Handles all Mailgun ETL",
    catchup=False,
    template_searchpath = [sql_dir],
    schedule_interval='0 * * * *'
) as dag:

    invoke_lambda_function = LambdaInvokeFunctionOperator.partial(
        task_id=f"execute_mailgun_lambdas",
        aws_conn_id='AmazonWebServices'
    ).expand(function_name=mailgun_event_load.mailgun_lambda_load)

    pause_for_pipe_stream_task = PythonOperator(
        task_id="Wait_for_Mailgun_Pipes_Streams_Tasks",
        python_callable=sleep,
        provide_context=True
    )

    create_to_create_batch = SQLExecuteQueryOperator.partial(
        task_id=f"call_mailgun.create_to_create_batch",
        conn_id='AWS_Snowflake',
        autocommit=True,
        sql=mailgun_event_load.create_to_create_batch
    ).expand(params=mailgun_event_load.recs)

    create_batch_to_stg_create = SQLExecuteQueryOperator.partial(
        task_id=f"call_mailgun.create_batch_to_stg_create",
        conn_id='AWS_Snowflake',
        autocommit=True,
        sql=mailgun_event_load.create_batch_to_stg_create
    ).expand(params=mailgun_event_load.recs)

    stg_create_to_DL = SQLExecuteQueryOperator.partial(
        task_id=f"call_mailgun.stg_create_to_DL",
        conn_id='AWS_Snowflake',
        autocommit=True,
        sql=mailgun_event_load.stg_create_to_DL
    ).expand(params=mailgun_event_load.recs)

    del_from_raw_and_create = SQLExecuteQueryOperator.partial(
        task_id=f"call_mailgun.del_from_raw_and_create",
        conn_id='AWS_Snowflake',
        autocommit=True,
        sql=mailgun_event_load.del_from_raw_and_create
    ).expand(params=mailgun_event_load.recs)

    invoke_lambda_function>>pause_for_pipe_stream_task>>create_to_create_batch >> create_batch_to_stg_create >> stg_create_to_DL >> del_from_raw_and_create