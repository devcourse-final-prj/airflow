from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import pendulum
from plugins import slack
import logging


def download_and_process_data(**context):
    local_tz = pendulum.timezone("Asia/Seoul")

    # 전체 날짜 범위 생성
    start_date = datetime(2014, 1, 1, tzinfo=local_tz).date()
    end_date = pendulum.now('Asia/Seoul').subtract(days=1).date()

    logging.info(f"날짜 범위 설정: {start_date} 부터 {end_date} 까지")
    dates = pd.date_range(start=start_date, end=end_date)

    # 데이터 다운로드
    dow = yf.download('^DJI',
                      start=start_date,
                      end=end_date - timedelta(days=1))
    nasdaq = yf.download('^IXIC',
                         start=start_date,
                         end=end_date - timedelta(days=1))
    forex_data = yf.download('USDKRW=X',
                             start=start_date,
                             end=end_date - timedelta(days=1))
    logging.info("데이터 다운로드 완료")

    # 'Date' 인덱스를 컬럼으로 변환 및 필요한 컬럼 선택
    dow.reset_index(inplace=True)
    nasdaq.reset_index(inplace=True)
    forex_data.reset_index(inplace=True)

    dow_selected = dow[['Date', 'Adj Close']] \
        .rename(columns={'Adj Close': 'dow'})

    nasdaq_selected = nasdaq[['Date', 'Adj Close']] \
        .rename(columns={'Adj Close': 'nasdaq'})

    forex_selected = forex_data[['Date', 'Adj Close']] \
        .rename(columns={'Adj Close': 'exchange'})

    # 모든 날짜를 포함하는 DataFrame 생성
    all_dates_df = pd.DataFrame(dates, columns=['Date'])

    # 병합하기 (모든 날짜 포함)
    merged_data = pd.merge(all_dates_df, dow_selected, on='Date', how='left')
    merged_data = pd.merge(merged_data, nasdaq_selected, on='Date', how='left')
    merged_data = pd.merge(merged_data, forex_selected, on='Date', how='left')
    logging.info("데이터 병합 완료")

    # 어제 데이터 가져오기
    dow_info = yf.Ticker("^DJI")
    nasdaq_info = yf.Ticker("^IXIC")
    forex_info = yf.Ticker("USDKRW=X")

    # 어제 데이터 업데이트
    last_row_index = merged_data.index[-1]

    # 각 지수의 최신 종가를 변수에 저장
    latest_dow_close = dow_info.history(period="1d")["Close"].iloc[-1]
    latest_nasdaq_close = nasdaq_info.history(period="1d")["Close"].iloc[-1]
    latest_exchange_close = forex_info.history(period="1d")["Close"].iloc[-1]

    # 변수 값을 merged_data에 할당
    merged_data.at[last_row_index, 'dow'] = latest_dow_close
    merged_data.at[last_row_index, 'nasdaq'] = latest_nasdaq_close
    merged_data.at[last_row_index, 'exchange'] = latest_exchange_close

    logging.info("최신 데이터 업데이트 완료")

    merged_data = merged_data.rename(columns={'Date': 'date'})
    merged_data = merged_data[['date', 'dow', 'nasdaq', 'exchange']]

    # 누락된 값을 바로 전날 데이터로 채우기
    merged_data.fillna(method='ffill', inplace=True)

    # 첫 번째 행에도 NaN 값이 있을 수 있으므로, 이를 제거
    merged_data.dropna(inplace=True)
    logging.info("누락된 값 처리 완료")

    logging.info(f"data 총 길이 : {len(merged_data)}")

    return merged_data


def upload_to_rds(**context):
    try:
        df_data = context["task_instance"] \
            .xcom_pull(task_ids="download_and_process")
        logging.info(f"data 총 길이 : {len(df_data)}")

        # MySQL Hook 사용하여 RDS 연결
        mysql_hook = MySqlHook(mysql_conn_id='aws_rds')
        engine = mysql_hook.get_sqlalchemy_engine()

        # 트랜잭션 시작
        with engine.begin() as connection:
            # 테이블 존재 여부 확인 및 삭제
            # SQL 쿼리를 변수에 저장
            check_table_query = "SHOW TABLES LIKE 'financial_data';"

            # 쿼리 실행 및 결과 저장
            table_exists = connection.execute(check_table_query).fetchone()

            if table_exists:
                connection.execute("DROP TABLE financial_data;")

            # 테이블 생성
            connection.execute('''
                CREATE TABLE IF NOT EXISTS `financial_data` (
                    `date` DATE NOT NULL,
                    `dow` FLOAT NULL DEFAULT NULL,
                    `nasdaq` FLOAT NULL DEFAULT NULL,
                    `exchange` FLOAT NULL DEFAULT NULL
                )
                COLLATE='utf8mb4_0900_ai_ci'
                ENGINE=InnoDB;
            ''')

            # 데이터프레임을 MySQL에 적재
            df_data.to_sql('financial_data',
                           con=connection,
                           if_exists='append',
                           index=False)

        logging.info("Data upload to RDS completed.")

    except Exception as e:
        logging.error(f"Error uploading data to RDS: {str(e)}")
        raise


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack.on_failure_callback,
}

dag = DAG(
    'daily_financial_data_to_rds',
    default_args=default_args,
    description='Downloads and processes dow, nosdaq,\
                        exchange rate data, then uploads to rds',
    schedule_interval='0 7 * * *',  # UTC 기준으로 매일 22시에 실행 (한국 시간 아침 7시)
    catchup=False,
)

download_and_process = PythonOperator(
    task_id='download_and_process',
    python_callable=download_and_process_data,
    dag=dag,
)

upload_to_rds_task = PythonOperator(
    task_id='upload_to_rds',
    python_callable=upload_to_rds,
    provide_context=True,  # context 제공 활성화
    dag=dag,
)

download_and_process >> upload_to_rds_task
