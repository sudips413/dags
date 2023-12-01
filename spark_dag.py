from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define your default_args, adjust them as per your requirements
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 1),
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1),
}

# Define your DAG
dag = DAG(
    'anish_spark_submit_dag',
    default_args=default_args,
    description='A DAG to test spark-submit command in usecase',
    schedule_interval='@daily',  # adjust the schedule_interval as per your requirements
)

# Define the SparkSubmitOperator task
spark_submit_task = SparkSubmitOperator(
    task_id='spark_submit_task',
    conn_id='spark',  # specify the connection id for your Spark cluster
    application="s3a://dn-apps-49baf552-2b21-40ba-832f-2392a4226235/scripts/spark/py/python_usecase.py",  # specify the path to your Spark script
    name='usecase_anish',
    verbose=False,
    conf={
        # 'spark.master': 'k8s://https://api.devbg.ooredoo.ps:6443',
        # 'spark.deploy-mode': 'cluster',
        'spark.executor.instances': '3',
        'spark.kubernetes.container.image': 'quay.io/dlytica_dev/spark-new/spark-py:v2',
        'spark.kubernetes.container.image.pullPolicy': 'IfNotPresent',
        'spark.kubernetes.container.image.pullSecrets': 'dlytica-dev-pull-secret',
        'spark.kubernetes.authenticate.driver.serviceAccountName': 'dn-spark-sa',
        'spark.kubernetes.namespace': 'dn-spark',
        'spark.kubernetes.local.dirs.tmpfs': 'true',
        'spark.eventLog.enabled': 'true',
        'spark.eventLog.dir': 's3a://dn-apps-49baf552-2b21-40ba-832f-2392a4226235/logs/spark/',
        'spark.hadoop.fs.s3a.endpoint': 'ocs-storagecluster-cephobjectstore-openshift-storage.apps.devbg.ooredoo.ps',
        'spark.hadoop.fs.s3a.access.key': 'F2TLWGDKJNRUCPE3XZ2B',
        'spark.hadoop.fs.s3a.secret.key': 'DdvWp9DIhlpMTQ0ZYC9MOJO5NFVGcpG24BhN2N2P',
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
