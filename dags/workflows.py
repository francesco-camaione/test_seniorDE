from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'francesco',
    'start_date': datetime(2023, 8, 30),
    'retries': 1,
}

# Instantiate the DAG
dag = DAG('de_test_workflow', default_args=default_args, schedule_interval=None)

spark_task = SparkSubmitOperator(
    task_id='sparkScript',
    conn_id='spark_standalone_conn',
    application="./app/main.py",
    executor_memory="3g",
    dag=dag
)

bash_task_1 = BashOperator(
    task_id='copyPartitionsDataIntoCSVFile',
    bash_command="cat /Users/france.cama/code/glovo_de_test/output_dataset/*.csv >> "
                 "/Users/france.cama/code/glovo_de_test/output_dataset/single_output_dataset_{{ ds_nodash }}.csv",
    dag=dag
)

# Task dependencies
spark_task >> bash_task_1
