from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator 


@dag(
    default_args={
        'owner': 'user',
        'retries': 0
    },
    schedule_interval='0 0 * * *'
)
def create_dag():

    # Run exist Glue Job
    run_exist_glue_job = GlueJobOperator(
        task_id='run_glue_job',
        script_args={
            'target_date': "{{ data_interval_start }}",
            'target_table': 'some_table'
        }
    )

    # Create Glue Job and run
    run_not_exist_glue_job = GlueJobOperator(
        task_id='run_not_exist_glue_job',
        script_location='s3://prd-s3-glue/jobs/some_job.py',
        script_args={
            'target_date': "{{ data_interval_start }}",
            'target_table': 'some_table'
        }
    )

    run_exist_glue_job >> run_not_exist_glue_job


create_dag()