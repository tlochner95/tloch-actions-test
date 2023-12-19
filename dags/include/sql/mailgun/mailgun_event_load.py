mailgun_lambda_load = ['CCLE-DEV-Mailgun-Accepted-Lambda',
        'CCLE-DEV-Mailgun-Clicked-Lambda',
        'CCLE-DEV-Mailgun-Complained-Lambda',
        'CCLE-DEV-Mailgun-Delivered-Lambda',
        'CCLE-DEV-Mailgun-Failed-Lambda',
        'CCLE-DEV-Mailgun-Opened-Lambda',
        'CCLE-DEV-Mailgun-Unsubscribed-Lambda']

recs = [{'event':'Accepted'},
        {'event':'Clicked'},
        {'event':'Complained'},
        {'event':'Delivered'},
        {'event':'Failed'},
        {'event':'Opened'},
        {'event':'Unsubscribed'}]

create_to_create_batch = """CALL \"CLARUS_DATALAKE_DEV\".\"STG\".\"DL_Mailgun_Wrk_{{params.event}}_Create_To_Create_Batch_SP\"();"""

create_batch_to_stg_create = """CALL \"CLARUS_DATALAKE_DEV\".\"STG\".\"DL_Mailgun_Wrk_{{params.event}}_Create_Batch_To_Stg_Create_SP\"();"""

stg_create_to_DL = """CALL \"CLARUS_DATALAKE_DEV\".\"STG\".\"DL_Mailgun_Stg_{{params.event}}_Create_To_DL_SP\"();"""

del_from_raw_and_create = """CALL \"CLARUS_DATALAKE_DEV\".\"STG\".\"DL_Mailgun_Wrk_{{params.event}}_Delete_From_Raw_and_Create_SP\"();"""