from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define your default_args, adjust them as per your requirements
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 4),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1),
}

# Define your DAG
dag = DAG(
    'spark_usecase_dag',
    default_args=default_args,
    description='A DAG to run spark usecase',
    schedule_interval='@daily',  # adjust the schedule_interval as per your requirements
)

# Define the SparkSubmitOperator task
spark_submit_task = SparkSubmitOperator(
    task_id='spark_usecase_task',
    conn_id='spark',  # specify the connection id for your Spark cluster
    application="s3a://dn-apps-a6636dd9-f954-48ef-a17a-2f6c07f1da1c/scripts/spark/py/python_usecase.py",  # specify the path to your Spark script
    # total_executor_cores='2',
    # executor_cores='1',
    # executor_memory='2g',
    # num_executors='3',
    # driver_memory='2g',
    name='usecase_anish',
    verbose=False,
    conf={
        'spark.executor.instances': '3',
        'spark.kubernetes.container.image': 'quay.io/dlytica_dev/spark:v1',
        'spark.kubernetes.container.image.pullPolicy': 'IfNotPresent',
        'spark.kubernetes.authenticate.driver.serviceAccountName': 'dn-spark-sa',
        'spark.kubernetes.namespace': 'dn-spark',
        'spark.kubernetes.local.dirs.tmpfs': 'true',
        'spark.eventLog.enabled': 'true',
        'spark.eventLog.dir': 's3a://dn-apps-a6636dd9-f954-48ef-a17a-2f6c07f1da1c/logs/spark/',
        'spark.hadoop.fs.s3a.endpoint': 's3-openshift-storage.apps.devbg.ooredoo.ps',
        'spark.hadoop.fs.s3a.access.key': '4rf1yAf6EyzBiNV3skKJ',
        'spark.hadoop.fs.s3a.secret.key': '+wlSneW2cq1TejTOkQEMnb0GIBbMRNcS/tz+Idw8',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false'
    },
    dag=dag,
)

# Set up task dependencies if needed
# task1 >> task2
spark_submit_task

if __name__ == "__main__":
    dag.cli()
