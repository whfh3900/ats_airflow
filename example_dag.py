from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'su.choi', # DAG 소유자 이름
    'depends_on_past': False, # DAG의 이전 실행결과에 영향을 받는지 여부
    'start_date': datetime(2023, 4, 28), # DAG가 시작될 시간을 나타내는 datetime 객체. 필수.
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
    'example_dag', # DAG의 고유한 식별자
    default_args=default_args, # DAG에 적용될 기본 변수를 정의한 사전(dict)
    schedule_interval='*/10 * * * *', #  DAG가 스케줄러에 의해 실행될 간격을 정의합니다. 예를 들어, schedule_interval='@daily'으로 설정하면 DAG는 매일 한번씩 실행됩니다.
    catchup=False, # DAG가 이전에 실행되지 않은 작업을 재실행할 것인지 여부
    description="example test dag", # DAG에 대한 설명
    max_active_runs=1, # DAG의 최대 동시 실행 횟수
    concurrency=3, # DAG의 최대 병렬 실행 횟수를 제한하는 정수 값
)

def extract_data():
    # 데이터 추출
    print("-"*10, "extract_data", "-"*10)
    data = ",".join([str(i) for i in list(range(1,100))])
    return data

def process_data(data):
    # 데이터 처리
    print("-"*10, "process_data", "-"*10)
    print("변환전", type(data), data)
    result = ",".join([str(int(i)**2) for i in data.split(',')])
    print("변환후",type(result), result)
    return result

def save_data(data):
    # 결과 저장
    print("-"*10, "save_data", "-"*10)
    data = data.split(",")
    with open('./result.txt', 'w') as f:
        for item in data:
            f.write("%s\n" % item)

task1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

task2 = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    op_kwargs={'data': "{{ ti.xcom_pull(task_ids='extract_data') }}"},
    dag=dag
)

task3 = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    op_kwargs={'data': "{{ ti.xcom_pull(task_ids='process_data') }}"},
    dag=dag
)

task1 >> task2 >> task3