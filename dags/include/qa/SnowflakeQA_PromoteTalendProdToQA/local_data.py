from datetime import datetime as dt
import json

prodToQAPromotion = '6463e81f5084007ec40fb302'

exec_promotion_data = json.dumps(
    {
        "executable": prodToQAPromotion,
        "keepTargetResources": True,
        "keepTargetRunProfiles": True,
        "context": f"Prod to QA promotion triggered via API on {dt.now()}"
    }
)

exec_promotion_endpoint = 'processing/executions/promotions'

promotion_execution_status_endpoint = 'executions/promotions/[[execId]]'
promotion_exec_status_poke_interval = 150
promotion_exec_status_check_timeout = 60 * 20

environments_query_endpoint = 'environments?query=name%3D%3DQA'

qa_schedules_endpoint = 'schedules/?environmentId=[[qaEnv]]'

unlink_task_from_schedule_endpoint = 'executables/tasks/[[executableId]]/schedule'

unlink_plan_from_schedule_endpoint = 'executables/plans/[[executableId]]/schedule'

del_schedule_endpoint = 'schedules/[[scheduleId]]'

plansToSchedule = [ # Plans to be scheduled in QA environment (plans from QA environment)
    '14ef468f-5dec-458c-bcd1-f0bf8ea7dea0', # Load EDW
    'ed2fb571-1e57-46d0-934b-aa6ccc010894', # DIM_PARTICIPANT
    '99b3b1f0-bf2a-43dd-b02e-61dc59296b00', # DIM_ENGAGEMENT
    '7e567a96-d3ed-4474-aec9-ce64bc6d5ba1', # Build Lookup Tables
    'a9e29113-6b74-450c-ad3b-7eb8751ccfc6', # Golden Schema Jobs
]