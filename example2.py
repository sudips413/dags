from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance
dag = DAG(
    'test_bash_dag',
    default_args=default_args,
    description='A simple DAG for testing with BashOperator',
    schedule_interval=timedelta(days=1),  # Set the schedule interval
)

# Define a BashOperator task that runs a simple Bash command
run_this = BashOperator(
    task_id='run_test_script',
    bash_command='echo "Hello from BashOperator!"',
    dag=dag,
)

# Set the task dependencies
run_this

if __name__ == "__main__":
    dag.cli()
