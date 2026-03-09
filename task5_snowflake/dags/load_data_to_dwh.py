from airflow.sdk import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def call_procedure(proc_name: str) -> None:
    """
    Execute a Snowflake stored procedure.

    :param proc_name: Snowflake stored procedure name.
    """

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    hook.run(f"CALL {proc_name}();", autocommit=True)


@dag(
    dag_id='load_data_to_dwh',
    catchup=False,
    max_active_runs=1,
)
def load_data_dwh():
    """
    Orchestrates the airline DWH pipeline in Snowflake.

    Flow:
    RAW → STAGE → DWH
    """

    @task(task_id="load_raw_data")
    def load_raw_data() -> None:
        """
        Load source data into the RAW layer and record load metadata in the logging table.
        """

        call_procedure("AIRLINE_DB.RAW.LOAD_RAW_AND_WRITE_LOG")

    @task(task_id="transform_raw_to_stage")
    def transform_raw_to_stage() -> None:
        """
        Transform RAW data into the STAGE layer.
        """

        call_procedure("AIRLINE_DB.STAGE.TRANSFORM_RAW_TO_STAGE")

    @task(task_id="load_stage_to_dwh")
    def load_stage_to_dwh() -> None:
        """
        Load curated data from STAGE into the DWH layer.
        """

        call_procedure("AIRLINE_DB.DWH.LOAD_STAGE_TO_DWH")

    load_raw_data() >> transform_raw_to_stage() >> load_stage_to_dwh()


load_data_dwh()
