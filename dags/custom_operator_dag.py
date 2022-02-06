""" Example Airflow DAG with custom plugin"""
import random
import time
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule


def return_branch(**kwargs):
    sleep_times = [5, 7, 9]
    branches = ['branch_0', 'branch_1', 'branch_2']
    time.sleep(random.choice(sleep_times))
    return random.choice(branches)

#
# def hello_world():
#     print("Congratulations! This is your first dag!")
#
#
# def goodbye_world():
#     print("Goodbye! Your second dag is done!")


# with DAG(
#         "custom_operator_dag",
#         schedule_interval=None,
#         start_date=days_ago(n=0, second=7),
#         dagrun_timeout=timedelta(minutes=60),
# ) as dag:
#     task1 = PythonOperator(
#         task_id="hello_world_task",
#         python_callable=hello_world
#     )
#
#     task2 = PythonOperator(
#         task_id="goodbye_world_task",
#         python_callable=goodbye_world
#     )
#
#     task1 >> task2

with DAG(dag_id='branch',
         start_date=days_ago(n=0, second=7),
         max_active_runs=1,
         schedule_interval=None,
         catchup=False
         ) as dag:
    # DummyOperators
    start = DummyOperator(task_id='start')
    end = DummyOperator(
        task_id='end',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=return_branch,
        provide_context=True
    )

    start >> branching

    for i in range(0, 3):
        d = DummyOperator(task_id='branch_{0}'.format(i))
        branching >> d >> end
