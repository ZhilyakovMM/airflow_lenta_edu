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
    'owner': 'Михаил Жиляков',
    'email': ['mikhail.zhilyakov@glowbyteconsulting.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': datetime.timedelta(minutes=5)
}

def example(dag_name: str, **kwargs):
    from airflow.providers.apache.impala.hooks.impala import ImpalaHook

    hook = ImpalaHook(
        conn_name_attr='impala_test',
        default_conn_name='impala_test'
    )
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(f'CREATE TABLE IF NOT EXISTS lenta_training.sales__{dag_name} AS SELECT * FROM lenta_training.sales LIMIT 100')


with DAG(
    dag_id=DAG_NAME,
    default_args=dag_default_args,
    max_active_runs=1,
    # Регламент каждые 3 часа
    schedule_interval="0 */3 * * *",
    start_date=pendulum.datetime(2023, 3, 21, tz="UTC"),
    catchup=False
):
    example_op = PythonOperator(
        task_id='example',
        python_callable=example,
        op_kwargs={'dag_name': DAG_NAME},
        provide_context=True
    )