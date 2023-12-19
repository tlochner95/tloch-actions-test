from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
import pendulum

import json
import os

import include.qa.SnowflakeQA_PromoteTalendProdToQA.local_data as local_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

base_dir = os.path.dirname(os.path.realpath(__file__))
include = os.path.join(base_dir, 'include')
sql_dir = os.path.join(include, 'sql')

talend_api = 'Talend_API'
talend_tmc_api = 'Talend_TMC_API'

def unlink_and_del_schedules(item, *args):
    scheduleId = item['id']
    if ('executableType' in item and 'executableId' in item):
        executableType = item['executableType']
        executableId = item['executableId']

        # Unlink the task from the schedule if this is a task
        if executableType == 'TASK':
            HttpHook(
                http_conn_id = talend_tmc_api,
                method='DELETE'
            ).run(
                endpoint=local_data.unlink_task_from_schedule_endpoint.replace("[[executableId]]", executableId)
            )

        # Unlink the plan from the schedule if this is a plan
        if executableType == 'PLAN':
            HttpHook(
                http_conn_id = talend_tmc_api,
                method='DELETE'
            ).run(
                endpoint=local_data.unlink_plan_from_schedule_endpoint.replace("[[executableId]]", executableId)
            )

        # Plan or task unlinked, now delete the schedule
        HttpHook(
                http_conn_id = talend_tmc_api,
                method='DELETE'
            ).run(
                endpoint=local_data.del_schedule_endpoint.replace("[[scheduleId]]", scheduleId)
            )

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    start_date=pendulum.datetime(2023, 12, 13, tz="America/New_York"),
    default_args=default_args,
    description=f"Handles the promoting of Prod Talend to QA",
    catchup=False,
    template_searchpath=[sql_dir],
    render_template_as_native_obj=True,
    schedule_interval='0 7 * * 2'
) as dag:
    trigger_prod_to_qa_promotion = SimpleHttpOperator(
        task_id="trigger_prod_to_qa_promotion",
        http_conn_id=talend_api,
        endpoint=local_data.exec_promotion_endpoint,
        method='POST',
        response_filter=lambda response: response.headers['Content-Location'],
        data=local_data.exec_promotion_data
    )

    wait_for_promotion_completion = HttpSensor(
        task_id="wait_for_promotion_completion",
        http_conn_id=talend_tmc_api,
        endpoint=local_data.promotion_execution_status_endpoint.replace('[[execId]]', str(trigger_prod_to_qa_promotion.output)),
        response_check=lambda response: json.loads(response.text)['status'] in ['PROMOTION_WARNING', 'PROMOTED'],
        mode='reschedule',
        timeout=local_data.promotion_exec_status_check_timeout,
        poke_interval=local_data.promotion_exec_status_poke_interval,
        do_xcom_push=True
    )

    get_qa_env_id = SimpleHttpOperator(
        task_id="get_qa_env_id",
        http_conn_id=talend_tmc_api,
        endpoint=local_data.environments_query_endpoint,
        method='GET',
        response_filter=lambda response: json.loads(response.text)[0]['id'],
        do_xcom_push=True
    )

    retrieve_schedules_in_qa = SimpleHttpOperator(
        task_id="retrieve_schedules_in_qa",
        http_conn_id=talend_tmc_api,
        endpoint=local_data.qa_schedules_endpoint.replace("[[qaEnv]]", str(get_qa_env_id.output)),
        method='GET',
        response_filter=lambda response: [[item] for item in json.loads(response.text)['items'] if True],
        do_xcom_push=True
    )

    unlink_and_del_schedules = PythonOperator.partial(
        task_id="unlink_and_del_schedules",
        python_callable=unlink_and_del_schedules
    ).expand(op_args=retrieve_schedules_in_qa.output)

    trigger_prod_to_qa_promotion >> wait_for_promotion_completion >> get_qa_env_id >> retrieve_schedules_in_qa >> unlink_and_del_schedules