from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


@dag(
    default_args={
        'owner': 'user',
        'retries': 0
    },
    schedule_interval='0 0 * * *'
)
def create_dag():

    @task
    def generate_glue_job_args(target_date: str, tables: list):
        glue_job_args = []
        for table in tables:
            glue_job_args.append({
                'script_args': {
                    'target_date': target_date,
                    'table': table,
                },
            })

    glue_job_args = generate_glue_job_args(
        target_date="{{ data_interval_start | ds_nodash }}",
        tables=['table_1', 'table_2', 'table_3']
    )

    (GlueJobOperator
        .partial(
            task_id='glue_job_runs',
            job_name='glue_job_name')
        .expand(glue_job_args)
    )

create_dag()