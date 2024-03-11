from airflow import DAG
import pandas as pd
from datetime import datetime
import pickle
from pykrx import stock
from io import StringIO
import logging
import os
from pendulum import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 로깅 설정
logger = logging.getLogger("airflow.task")


def read_csv_from_s3(bucket_name, key, **kwargs):
    try:
        hook = S3Hook(aws_conn_id="aws_conn")  # Airflow Connection ID 지정
        content = hook.read_key(key, bucket_name)  # S3에서 파일 내용 읽기
        content_str = StringIO(content)
        df = pd.read_csv(content_str, dtype={'stock_code': str})

        # DataFrame을 임시 파일에 저장
        file_path = os.path.join('data', "closing_prices_history.csv")
        df.to_csv(file_path, index=False)

        # 파일 경로를 XCom에 push
        kwargs['ti'].xcom_push(key='file_path', value=file_path)
        logger.info(f"DataFrame saved to {file_path} and path pushed to XCom.")

    except Exception as e:
        logger.error(f"Failed to read CSV from S3: {e}", exc_info=True)
        raise


# 데이터 처리 함수
def process_data(**kwargs):
    execution_date = kwargs.get('next_execution_date')  # execution_date 가져오기
    today_formatted = execution_date.strftime('%Y-%m-%d')  # execution_date를 YYYY-MM-DD 형태로 변환
    logger.info(f"작업 실행 날짜: {today_formatted}")

    try:
        # pickle 파일에서 index_search 딕셔너리 불러오기
        with open('data/index_search.pkl', 'rb') as pickle_file:
            index_search = pickle.load(pickle_file)

        # closing_prices_history 불러오기
        ti = kwargs['ti']
        file_path = ti.xcom_pull(task_ids='read_csv_from_s3', key='file_path')

        if file_path is None:
            logger.error("No file path returned from read_csv_from_s3 task.")
            raise AirflowFailException("No file path returned from read_csv_from_s3 task.")

        closing_prices_history = pd.read_csv(file_path)
        closing_prices_history['date_column'] = pd.to_datetime(closing_prices_history['date_column'])
        closing_prices_history = closing_prices_history.sort_values(by=['sector_name', 'stock_code', 'kr_stock_name', 'date_column'])

        new_rows = []  # 추가할 새로운 데이터를 저장할 리스트

        for (sector_code, sector_name), stocks in index_search.items():
            for stock_code, kr_stock_name in stocks:
                # 주식의 과거 OHLCV 데이터를 가져옴
                df = stock.get_market_ohlcv_by_date(today_formatted, today_formatted, stock_code)

                if df.empty:
                    logger.warning(f"데이터가 없습니다: {sector_code}, {stock_code}, {kr_stock_name}")
                    continue

                # 이미 존재하는 데이터인지 확인
                query = f"stock_code == '{stock_code}' & date_column == '{today_formatted}'"
                if not closing_prices_history.query(query).empty:
                    logger.info(f"이미 존재하는 데이터: {sector_code}, {stock_code}, {kr_stock_name}")
                    continue

                # 데이터 추가
                logger.info(f"데이터 추가됨: {sector_code}, {stock_code}, {kr_stock_name}")
                new_row = {
                    'sector_name': sector_name,
                    'stock_code': stock_code,
                    'kr_stock_name': kr_stock_name,
                    'date_column': pd.to_datetime(today_formatted),
                    'closing_price': df.iloc[0]['종가']
                }
                new_rows.append(new_row)

        if new_rows:
            # 새로운 데이터를 DataFrame에 추가
            new_data = pd.DataFrame(new_rows)
            closing_prices_history = pd.concat([closing_prices_history, new_data], ignore_index=True)
            closing_prices_history['stock_code'] = closing_prices_history['stock_code'].apply(lambda x: str(x).zfill(6))
            closing_prices_history.drop_duplicates(subset=['sector_name', 'stock_code', 'kr_stock_name', 'date_column'], inplace=True)
            closing_prices_history = closing_prices_history.sort_values(by=['sector_name', 'stock_code', 'kr_stock_name', 'date_column'])
            closing_prices_history['closing_price'] = closing_prices_history['closing_price'].astype(int)
            closing_prices_history.to_csv("data/closing_prices_history.csv", index=False)

    except ValueError as e:
        logger.error(f"Data processing error: {e}", exc_info=True)
        raise AirflowFailException(f"Data processing error: {e}")


def upload_csv_to_S3(bucket_name, key, **kwargs):
    try:
        hook = S3Hook(aws_conn_id="aws_conn")  # Airflow Connection ID 지정
        hook.load_file(
            filename="data/closing_prices_history.csv",
            bucket_name=bucket_name,
            replace=True,
            key=key
            )

    except Exception as e:
        logger.error(f"Failed to upload CSV to S3: {e}", exc_info=True)
        raise


def upload_to_gcp_sql():

    # 데이터베이스 설정
    hook = PostgresHook(postgres_conn_id='postgres_conn')
    # 사용 예: 커넥션을 이용한 데이터베이스 작업
    conn = hook.get_conn()

    file_path = 'data/closing_prices_history.csv'

    try:
        # 데이터베이스에 연결
        cursor = conn.cursor()

        # 트랜잭션 시작
        conn.autocommit = False

        # 기존 데이터 삭제
        cursor.execute("DELETE FROM viz_dailystockprice")

        # COPY 명령을 사용하여 CSV 파일에서 데이터베이스로 데이터 삽입
        with open(file_path, 'r') as f:
            cursor.copy_expert(sql="COPY viz_dailystockprice(sector_name, stock_code, kr_stock_name, date_column, closing_price) FROM STDIN WITH CSV HEADER", file=f)

        # 트랜잭션 커밋
        conn.commit()

    except Exception as e:
        # 예외 발생 시 롤백
        conn.rollback()
        logger.error("Data COPY processing error: %s", e, exc_info=True)
        raise AirflowFailException(f"Data COPY processing error: {e}")

    finally:
        # 리소스 정리
        cursor.close()
        conn.close()


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 5, tzinfo=timezone('Asia/Seoul')),
    "retries": 1,
}


dag = DAG(
    "stock_daily_closing_price",
    default_args=default_args,
    catchup=False,
    schedule_interval="0 18 * * 1-5",
    tags=["stock"],
    max_active_runs=1,
)

read_csv_from_s3 = PythonOperator(
    task_id='read_csv_from_s3',
    python_callable=read_csv_from_s3,
    op_kwargs={
        'bucket_name': 'de-4-3-bucket',
        'key': 'raw_data/closing_prices_history.csv'
        },
    dag=dag,
)

process_data = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

upload_csv_to_S3 = PythonOperator(
    task_id='upload_csv_to_S3',
    python_callable=upload_csv_to_S3,
    op_kwargs={
        'bucket_name': 'de-4-3-bucket',
        'key': 'raw_data/closing_prices_history.csv'
        },
    dag=dag,
)

upload_to_gcp_sql = PythonOperator(
    task_id='upload_to_gcp_sql',
    python_callable=upload_to_gcp_sql,
    dag=dag,
)

read_csv_from_s3 >> process_data >> [upload_csv_to_S3, upload_to_gcp_sql]
