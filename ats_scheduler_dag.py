from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from ats_schedule_tasks import *

default_args = {
    'owner': 'su.choi', # DAG 소유자 이름
    'depends_on_past': False, # DAG의 이전 실행결과에 영향을 받는지 여부
    'start_date': datetime(2023, 6, 1, 16, 18, 0), # DAG가 시작될 시간을 나타내는 datetime 객체. 필수.
    'retries': 1, # DAG 실행이 실패한 경우 재시도할 횟수
    'retry_delay': timedelta(minutes=10), # DAG 실행이 실패했을 때 재시도 간격을 나타내는 timedelta 객체
    'email': 'su.choi@niccompany.co.kr', # DAG에 관련된 이메일 주소
    'email_on_failure': False, # DAG 실행이 실패했을 때 이메일을 보낼지 여부
    'email_on_retry': False, # DAG 실행이 재시도될 때 이메일을 보낼지 여부
    # 'queue': 'test_queue', # DAG 실행을 위해 사용될 큐의 이름
    # 'pool': 'test_pool', # DAG 실행을 위해 사용될 풀의 이름
    # 'end_date': datetime(2023, 4, 29), # DAG의 종료 시간을 나타내는 datetime 객체 
}

dag = DAG(
    'ATS_SCHEDULER', # DAG의 고유한 식별자
    default_args=default_args, # DAG에 적용될 기본 변수를 정의한 사전(dict)
    schedule_interval='*/10 * * * *', #  DAG가 스케줄러에 의해 실행될 간격을 정의합니다. 예를 들어, schedule_interval='@daily'으로 설정하면 DAG는 매일 한번씩 실행됩니다.
    catchup=True, # DAG가 이전에 실행되지 않은 작업을 재실행할 것인지 여부
    description="Read the 'Real?ATS' database and process the tasks that need to be handled in that data sequentially.", # DAG에 대한 설명
    max_active_runs=1, # DAG의 최대 동시 실행 횟수
    concurrency=1, # DAG의 최대 병렬 실행 횟수를 제한하는 정수 값
)

task1 = BranchPythonOperator(
    task_id='connect_mysql_return_values',
    python_callable=connect_mysql_return_values,
    dag=dag
)

task2 = BranchPythonOperator(
    task_id='text_preprocessing',
    python_callable=preprocessing,
    provide_context=True,
    dag=dag
)

task3 = BranchPythonOperator(
    task_id='text_classification',
    python_callable=text_classification,
    provide_context=True,
    dag=dag
)

task4 = BranchPythonOperator(
    task_id='file_transfer',
    python_callable=file_transfer,
    provide_context=True,
    dag=dag
)

task5 = BranchPythonOperator(
    task_id='file_remove',
    python_callable=file_remove,
    dag=dag
)

task6 = PythonOperator(
    task_id='complete',
    python_callable=complete,
    dag=dag
)

pass_task = ShortCircuitOperator(
    task_id='task_pass',
    python_callable=task_pass,
    dag=dag
)

error_task = ShortCircuitOperator(
    task_id='task_error',
    python_callable=task_error,
    dag=dag
)


task1 >> [task2, pass_task] 
task2 >> [task3, error_task] 
task3 >> [task4, error_task] 
task4 >> [task5, error_task]
task5 >> [task6, error_task]