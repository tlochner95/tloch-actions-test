sources_query = """SELECT DISTINCT SOURCE_DB_NAME, SOURCE_TABLE_NAME
    FROM CLARUS_DATALAKE_DEV.DRV."DL_Clarus_LoyaltyBurst_SourceTableTracking"
    WHERE IS_CURRENT = TRUE
    AND IS_READY_FOR_DL_INGESTION = TRUE"""

create_to_create_batch = """CALL \"CLARUS_DATALAKE_DEV\".\"STG\".\"DL_LoyaltyBurst_{{params.db_name}}_Wrk_{{params.tbl_name}}_Create_To_Create_Batch_SP\"();"""

create_batch_to_stg_create = """CALL \"CLARUS_DATALAKE_DEV\".\"STG\".\"DL_LoyaltyBurst_{{params.db_name}}_Wrk_{{params.tbl_name}}_Create_Batch_To_Stg_Create_SP\"();"""

stg_create_to_DL = """CALL \"CLARUS_DATALAKE_DEV\".\"STG\".\"DL_LoyaltyBurst_{{params.db_name}}_Stg_{{params.tbl_name}}_Create_To_DL_SP\"();"""

del_from_raw_and_create = """CALL \"CLARUS_DATALAKE_DEV\".\"STG\".\"DL_LoyaltyBurst_{{params.db_name}}_Wrk_{{params.tbl_name}}_Delete_From_Raw_and_Create_SP\"();"""