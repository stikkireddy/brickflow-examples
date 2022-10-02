import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

dag = DAG(
    dag_id='example_1',
    schedule_interval='30 9,18,23 * * *',
    start_date=datetime.datetime(2021, 1, 1),
)

def branch_func(**kwargs):
    ti = kwargs['ti']
    xcom_value = int(ti.xcom_pull(task_ids='start_task'))
    if xcom_value >= 5:
        return 'continue_task'
    else:
        return 'stop_task'

start_op = BashOperator(
    task_id='start_task',
    bash_command="echo 5",
    xcom_push=True,
    dag=dag)

branch_op = BranchPythonOperator(
    task_id='branch_task',
    provide_context=True,
    python_callable=branch_func,
    dag=dag)

dummy_op = BashOperator(
    task_id='dummy_task',
    bash_command="echo dummy",
    xcom_push=True,
    dag=dag)

continue_op = BashOperator(
    task_id='continue_task',
    bash_command="echo continue",
    xcom_push=True,
    dag=dag)

continue2_op = BashOperator(
    task_id='continue2_task',
    bash_command="echo continue2",
    xcom_push=True,
    dag=dag)

continue3_op = BashOperator(
    task_id='continue3_task',
    bash_command="echo continue3",
    xcom_push=True,
    dag=dag)

    # DummyOperator(task_id='continue_task', dag=dag)
stop_op = BashOperator(
    task_id='stop_task',
    bash_command="echo stop",
    xcom_push=True,
    dag=dag)
no_op = DummyOperator(task_id='no_op', dag=dag)

stop2_op = BashOperator(
    task_id='stop2_task',
    bash_command="echo stop2",
    xcom_push=True,
    dag=dag)


join_op = BashOperator(
    task_id='join_task',
    # sla=203,
    bash_command="echo 'join task'",
    trigger_rule=TriggerRule.NONE_FAILED,
    xcom_push=True,
    dag=dag)