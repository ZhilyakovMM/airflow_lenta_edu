import datetime
import os
import sys
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_NAME = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
# Add python path
PYTHON_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__))).replace('\\', '/')
sys.path.append(PYTHON_PATH)

dag_default_args = {
    'owner': 'Zhilyakov Mikhail',
    'email': ['mikhail.zhilyakov@glowbyteconsulting.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': datetime.timedelta(minutes=5)
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=dag_default_args,
    max_active_runs=1,
    # Регламент каждые 3 часа
    schedule_interval="0 */3 * * *",
    start_date=pendulum.datetime(2023, 3, 21, tz="UTC"),
    catchup=False
)

def example(**kwargs):
    return {'key_example': datetime.datetime.now().strftime('YYYY-MM-DD HH:MI:SS')}


example_op = PythonOperator(
    dag=dag,
    task_id=f"{example.__name__}",
    do_xcom_push=True,
    python_callable=example,
    op_kwargs={'example_kwarg': 'example'},
    provide_context=True
)