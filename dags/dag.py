from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

s3_script = "spark/etl.py"

# DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2020, 10, 17),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "deutsche_boerse_spark_job",
    default_args=default_args,
    schedule_interval="@once",
    max_active_runs=1,
)

# Start
start_pipeline = DummyOperator(task_id="start_pipeline", dag=dag)


# Dump to pyspark script and dimension_table to the s3 bucket for EMR Cluster to fetch from
def dump_to_s3(filename, key, bucket_name):
    """
    Loads files to a specified s3 bucket
    :param filename: path and filename
    :param key: key path on s3_bucket to load file onto
    :param bucket_name: s3 bucket name to load file onto
    """
    s3 = S3Hook()
    s3.load_file(filename=filename, bucket_name=bucket_name, replace=True, key=key)


script_to_s3 = PythonOperator(
    dag=dag,
    task_id="dump_etl_to_s3",
    python_callable=dump_to_s3,
    op_kwargs={
        "filename": "./dags/spark/etl.py",
        "key": "spark/etl.py",
        "bucket_name": Variable.get("s3_bucket"),
    },
)

dimension_table_to_s3 = PythonOperator(
    dag=dag,
    task_id="dump_dimension_table_to_s3",
    python_callable=dump_to_s3,
    op_kwargs={
        "filename": "./dags/dimension_data/eurex_product_specification.csv",
        "key": "dimension_table/eurex_product_specification.csv",
        "bucket_name": Variable.get("s3_bucket"),
    },
)

# Launch EMR Cluster
JOB_FLOW_OVERRIDES = {
    "Name": "Deutsche-Boerse-ETL",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        "PYSPARK_PYTHON": "/usr/bin/python3",
                        "OUTPUT_URI": Variable.get("s3_bucket")
                    },
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 4,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

launch_emr_cluster = EmrCreateJobFlowOperator(
    task_id="launch_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
)


# Add steps to EMR job flow
SPARK_STEPS = [
    {
        "Name": "ETL for Deutsche Boerse data",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "s3://{{ params.s3_bucket }}/{{ params.s3_script }}",
            ],
        },
    },
]

step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='launch_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    params={
        "s3_bucket": Variable.get("s3_bucket"),
        "s3_script": s3_script,
    },
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1

step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('launch_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[" + str(last_step) + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='launch_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# End
end_pipeline = DummyOperator(task_id="end_pipeline", dag=dag)

# Task dependencies
start_pipeline >> script_to_s3 >> launch_emr_cluster
start_pipeline >> dimension_table_to_s3 >> launch_emr_cluster
launch_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> end_pipeline
