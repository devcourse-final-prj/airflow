from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# 이 함수는 작업이 성공적으로 완료되었을 때 호출됩니다.
def print_completed_dags():
    print("completed DAGs!!!!")

# 기본 인수 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 4),  # 오늘 날짜로 설정
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'end_date': datetime(2023, 3, 5),  # 필요한 경우 종료 날짜를 설정할 수 있습니다.
}

# DAG 정의
dag = DAG(
    'airflow_test_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),  # 매일 실행
    catchup=False,
)

# PythonOperator 정의
complete_task = PythonOperator(
    task_id='print_complete_message',
    python_callable=print_completed_dags,
    dag=dag,
)

complete_task  # 이것은 단지 작업을 DAG에 할당합니다.

