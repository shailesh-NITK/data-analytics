from datetime import datetime,timedelta

from airflow import DAG
import os
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from cosmos.providers.dbt.task_group import DbtTaskGroup
from cosmos.providers.dbt.core.operators import (
    DbtDepsOperator,
    DbtRunOperationOperator,
    DbtSeedOperator,
    DbtRunOperator,
)
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)


default_args = {
    'owner': 'me',
    'retries': 0,
    'start_date':datetime(2023,1,1)
}

DBT_PROJECT_NAME = "telocom_dbt"
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"
DBT_ROOT_PATH = "/usr/local/airflow/dbt/telecom-dbt"
base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ge_root_dir = os.path.join(base_path,'include','great_expectations')
print("ge_root_dir", ge_root_dir)


dag = DAG(
    dag_id="shailesh_dag", default_args=default_args, schedule_interval=None
)

snowflake_query = [
        """
            create  table if not exists source_table( emp_no int,emp_name text,salary int, hra int ,Dept text);
        """,
                    """INSERT INTO source_table VALUES (100, 'A' ,2000, 100,'HR'),
                        (101, 'B' ,5000, 300,'HR'),
                        (102, 'C' ,6000, 400,'Sales'),
                        (103, 'D' ,500, 50,'Sales'),
                        (104, 'E' ,15000, 3000,'Tech'),
                        (105, 'F' ,150000, 20050,'Tech'),
                        (105, 'F' ,150000, 20060,'Tech');
                  """
]



with dag:

    # create_table = SnowflakeOperator(
    #         task_id="create_table",
    #         sql=snowflake_query[0] ,
    #         snowflake_conn_id="snowflake_conn"
    #     )
        
    # insert_data = SnowflakeOperator(
    #     task_id="insert_snowflake_data",
    #     sql=snowflake_query[1] ,
    #     snowflake_conn_id="snowflake_conn"
    # )

    with TaskGroup(group_id='load_data') as load_data:

        copy_into_revenue_table = S3ToSnowflakeOperator(
            task_id="copy_into_revenue_table",
            snowflake_conn_id='snowflake_conn',
            s3_keys=['rev1.csv'],
            table='REVENUE',
            stage='snow_stage',
            file_format="(type = csv ,field_delimiter = ',',skip_header=1,null_if = ('NULL', 'null'), empty_field_as_null = true,FIELD_OPTIONALLY_ENCLOSED_BY='\"')"
        )

        copy_into_crm_table = S3ToSnowflakeOperator(
            task_id="copy_into_crm_table",
            snowflake_conn_id='snowflake_conn',
            s3_keys=['crm1.csv'],
            table='CRM',
            stage='snow_stage',
            file_format="(type = csv ,field_delimiter = ',',skip_header=1,null_if = ('NULL', 'null'), empty_field_as_null = true,FIELD_OPTIONALLY_ENCLOSED_BY='\"')"
        )

        copy_into_device_table = S3ToSnowflakeOperator(
            task_id="copy_into_device_table",
            snowflake_conn_id='snowflake_conn',
            s3_keys=['device1.csv'],
            table='DEVICE',
            stage='snow_stage',
            file_format="(type = csv ,field_delimiter = ',',skip_header=1,null_if = ('NULL', 'null'), empty_field_as_null = true,FIELD_OPTIONALLY_ENCLOSED_BY='\"')"
        )

        [copy_into_revenue_table, copy_into_crm_table, copy_into_device_table]


    with TaskGroup(group_id='clean_data') as clean_data:

        clean_data_pyspark = SparkSubmitOperator(
            task_id = 'clean_data_pyspark',
            dag=dag,
            conn_id = 'spark_local',
            #jars='/include/aws-java-sdk-1.12.368.jar,/include/aws-java-sdk-bundle-1.12.368.jar,/include/hadoop-aws-3.2.2.jar',
            jars = 'include/snowflake-jdbc-3.13.14.jar,include/spark-snowflake_2.12-2.10.0-spark_3.2.jar',
            executor_cores=4,
            executor_memory='8G',
            driver_memory='8G',
            application = 'pyspark_jobs/clean_data.py'
        )

        [clean_data_pyspark]


    with TaskGroup(group_id='curated_data') as curated_data:

        curated_data_transform = DbtRunOperator(
            task_id = "curated_data_transform",
            dag=dag,
            conn_id = 'snowflake_conn',
            dbt_executable_path=DBT_EXECUTABLE_PATH,
            project_dir = DBT_ROOT_PATH,
            select = 'telecom_analysis.sql',
            schema='CURATED',
            trigger_rule='all_done'
        )

        [curated_data_transform]

    with TaskGroup(group_id='validate_data') as validate_data:

        gx_validate = GreatExpectationsOperator(
            task_id='gx_validate',
            conn_id='snowflake_conn',
            data_context_root_dir=ge_root_dir,
            data_asset_name="CLEANSED.TEMP",
            expectation_suite_name="telecom_analysis.crm"
        )

        [gx_validate]

    with TaskGroup(group_id='results') as results:

        result_pyspark_load_snoflake = SparkSubmitOperator(
            task_id = 'result_pyspark_load_snoflake',
            dag=dag,
            conn_id = 'spark_local',
            #jars='/include/aws-java-sdk-1.12.368.jar,/include/aws-java-sdk-bundle-1.12.368.jar,/include/hadoop-aws-3.2.2.jar',
            jars = 'include/snowflake-jdbc-3.13.14.jar,include/spark-snowflake_2.12-2.10.0-spark_3.2.jar',
            executor_cores=4,
            executor_memory='8G',
            driver_memory='8G',
            application = 'pyspark_jobs/results.py'
        )

        [result_pyspark_load_snoflake]

    # crm_data_transform = DbtTaskGroup(
    #     group_id="transform_data",
    #     dbt_project_name=DBT_PROJECT_NAME,
    #     conn_id="snowflake_conn",
    #     dbt_root_path=DBT_ROOT_PATH,
    #     dbt_args={
    #         "dbt_executable_path": DBT_EXECUTABLE_PATH,
    #         "project_dir":f"/usr/local/airflow/dbt/models/telecom"
    #     },
    # )

load_data >> clean_data >> validate_data >> curated_data >> results
#results
