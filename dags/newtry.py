import textwrap 
from datetime import datetime, timedelta ##Timedelta for cauculating time
from airflow import DAG
from airflow.operators.bash import BashOperator ## use for call API or run bash command
default_args = {
    "depend_on_past" : False, ## depend on past task
    'owner' : "airflow", ## owner this task
    'start_date' : None, ## Date start run
    'end_date' : None, ## date end run
    'email' : None, ## email to send
    'email_on_failure' : False, ## send email when task fail
    'email_on_retry' : False, ## send email when task retry
}
dag = DAG(
    'newtry', ## name of task
    schedule="@daily",  # Chạy hằng ngày
    start_date=datetime(2024, 3, 30),  # Ngày bắt đầu chạy DAG
    catchup=False  # Không chạy lại các lần trước start_date
)
task1 = BashOperator(
    task_id="hello_task",
    bash_command="echo Hello Airflow!",
    dag=dag
)