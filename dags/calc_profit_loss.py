from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from pykrx import stock
from datetime import timedelta
import pandas as pd
import numpy as np
import datetime, decimal, json, os


# 기존에 제공된 종목 검색 함수
def list_indices_and_stocks(market):
    indices = stock.get_index_ticker_list(market=market)
    index_search = {}
    for idx in indices[1:18]:
        index_name = stock.get_index_ticker_name(idx)
        indice_name = idx, index_name.split()[1]
        stock_code = stock.get_index_portfolio_deposit_file(idx)

        stocks_name = []
        for code in stock_code[0:5]:
            stock_name = stock.get_market_ticker_name(code)
            stocks_name.append(stock_name)

        tickers = []
        for pair in zip(stock_code, stocks_name):
            tickers.append(pair)

        index_search[indice_name] = tickers

    return index_search


def fetch_price_data():
    indices_stocks = list_indices_and_stocks("KRX")
    periods = [7, 14, 30, 90, 180, 365]
    today = datetime.datetime.now()
    # 주말일 경우 금요일로 조정
    if today.weekday() > 4:
        today -= timedelta(days=today.weekday() - 4)
    formatted_today = today

    price_data = {}
    for (index_code, sector_name), stocks in indices_stocks.items():
        sector_prices = {}
        for stock_code, stock_name in stocks:
            stock_prices = {}

            # 오늘의 OHLCV 데이터 추출
            ohlcv_today = stock.get_market_ohlcv_by_date(
                fromdate=formatted_today.strftime("%Y%m%d"),
                todate=formatted_today.strftime("%Y%m%d"),
                ticker=stock_code,
            )
            current_price = (
                ohlcv_today["종가"].iloc[-1]
                if not ohlcv_today.empty and pd.notnull(ohlcv_today["종가"].iloc[-1])
                else None
            )
            stock_prices["현재가"] = current_price

            # 과거 날짜의 OHLCV 데이터 추출
            for period in periods:
                past_date = formatted_today - timedelta(days=period)
                average_price = None
                for attempt in range(3):
                    formatted_past_date = past_date.strftime("%Y%m%d")
                    ohlcv_past = stock.get_market_ohlcv_by_date(
                        fromdate=formatted_past_date,
                        todate=formatted_past_date,
                        ticker=stock_code,
                    )
                    if not ohlcv_past.empty and pd.notnull(ohlcv_past["종가"].iloc[-1]):
                        average_price = (
                            ohlcv_past["시가"].iloc[-1] + ohlcv_past["종가"].iloc[-1]
                        ) / 2
                        break
                    past_date -= timedelta(days=1)

                if average_price is None:
                    average_price = 0  # 데이터가 없으면 0 할당
                stock_prices[f"{period}일전 값의 평균 기준"] = average_price

            sector_prices[(stock_code, stock_name)] = stock_prices
        price_data[(index_code, sector_name)] = sector_prices

    return price_data


def calculate_profit_loss(price_data, investment_per_stock=2000000):
    results = {}

    for sector_info, stocks in price_data.items():
        sector_results = []

        for stock_info, stock_prices in stocks.items():
            stock_result = {
                "종목코드": stock_info[0],
                "종목명": stock_info[1],
                "현재가": stock_prices.get("현재가"),
                "수익": {},
                "투자 금액": {},
            }

            for time_period in stock_prices.keys():
                if "일전 값의 평균 기준" in time_period:
                    historical_price = stock_prices[time_period]
                    if historical_price == 0:
                        stock_result["수익"][time_period] = 0
                        stock_result["투자 금액"][time_period] = 0
                    else:
                        num_shares = investment_per_stock // historical_price
                        invested_amount = num_shares * historical_price
                        stock_result["투자 금액"][time_period] = invested_amount

                        if stock_prices.get("현재가") is not None:
                            current_value = num_shares * stock_prices["현재가"]
                            stock_result["수익"][time_period] = (
                                current_value - invested_amount
                            )
                        else:
                            stock_result["수익"][time_period] = 0

            sector_results.append(stock_result)

        sector_name = sector_info[1]
        results[sector_name] = sector_results

    return results


def serialize_results(results):
    """
    모든 결과 값을 JSON 직렬화 가능한 형식으로 변환하는 함수
    """
    if isinstance(results, dict):
        return {k: serialize_results(v) for k, v in results.items()}
    elif isinstance(results, list):
        return [serialize_results(v) for v in results]
    elif isinstance(results, (datetime.datetime, datetime.date)):
        return results.isoformat()
    elif isinstance(results, decimal.Decimal):
        return float(results)
    elif isinstance(results, (np.int32, np.int64)):  # numpy int 타입 추가
        return int(results)
    else:
        return results


def execute_and_save():
    """
    ETL data s3에 저장
    """
    price_data = fetch_price_data()
    cal_results = calculate_profit_loss(price_data)
    results = serialize_results(cal_results)

    filename = "/home/ubuntu/airflow/data/krx_calculation_data.json"
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=4)
        print("### Json으로 저장.")
    except TypeError as e:
        print(f"Json으로 저장시 발생한 에러 : {e}")

    try:
        s3_hook = S3Hook(aws_conn_id="s3_conn")
        bucket_name = "de-4-3-bucket"
        s3_key = "airflow/data/krx_calculation_data.json"

        s3_hook.load_file(filename, key=s3_key, bucket_name=bucket_name, replace=True)
        print("### save_to_s3 가 완료되었습니다.")
    except Exception as e:
        print(f"### S3에 업로드 시 발생한 에러 : {e}")


default_args = {
    "owner": "hoon",
    "depends_on_past": False,
    "start_date": datetime.datetime.combine(
        datetime.datetime.now().date(), datetime.datetime.min.time()
    ),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "stock_analysis_dag",
    default_args=default_args,
    description="Fetch stock data, calculate profit/loss and save to S3",
    schedule_interval="50 15 * * *",
)

execute_task = PythonOperator(
    task_id="execute_and_save_to_s3",
    python_callable=execute_and_save,
    dag=dag,
)
