import datetime
import os
import sys
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAG_NAME = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
# Add python path
PYTHON_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__))).replace('\\', '/')
sys.path.append(PYTHON_PATH)

dag_default_args = {
    'owner': 'Zhilyakov Mikhail',
    'email': ['mikhail.zhilyakov@glowbyteconsulting.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5)
}

spark_conf = Variable.get(key=DAG_NAME, deserialize_json = True)

with DAG(
    dag_id=DAG_NAME,
    default_args=dag_default_args,
    max_active_runs=1,
    # Регламент каждые 3 часа
    schedule_interval="0 */3 * * *",
    start_date=pendulum.datetime(2023, 3, 21, tz="UTC"),
    catchup=False
):
    spark_submit_op = SparkSubmitOperator(
        application="/opt/airflow/dags/repo/src/spark_app.py",
        conn_id='spark_k8s',
        task_id='spark_submit_example',
        application_args=[
            'lenta_training',
            'sales',
            'trainer'
        ],
        conf=spark_conf,
        name=f'{DAG_NAME}__spark_submit_example'
    )