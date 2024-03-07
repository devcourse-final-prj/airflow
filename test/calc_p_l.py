from pykrx import stock
from datetime import datetime, timedelta
import pandas as pd
import time


# 주어진 시장에서 지수 및 주식 목록을 가져오는 함수
def list_indices_and_stocks(market):
    indices = stock.get_index_ticker_list(market=market)
    index_search = {}
    for idx in indices[1:2]:
        index_name = stock.get_index_ticker_name(idx)
        indice_name = (idx, index_name.split()[1])
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


# 수정된 fetch_price_data 함수
def fetch_price_data():
    indices_stocks = list_indices_and_stocks("KRX")
    periods = [7, 14, 30, 90, 180, 365]  # 정의된 기간
    today = datetime.now().strftime("%Y%m%d")  # 오늘 날짜

    price_data = {}
    for (index_code, sector_name), stocks in indices_stocks.items():
        sector_prices = {}
        for stock_code, stock_name in stocks:
            # 오늘 날짜의 OHLCV 데이터를 가져옵니다.
            ohlcv_today = stock.get_market_ohlcv_by_date(
                fromdate=today, todate=today, ticker=stock_code
            )
            # krx 서버가 차단할 수 있음.
            time.sleep(0.5)
            # 오늘의 종가가 있으면 사용하고, 없으면 시가를 사용합니다.
            if not ohlcv_today.empty:
                if pd.isna(ohlcv_today["종가"].iloc[-1]):
                    current_price = ohlcv_today["시가"].iloc[-1]
                else:
                    current_price = ohlcv_today["종가"].iloc[-1]
            else:
                current_price = None

            stock_prices = {"현재가": current_price}
            for period in periods:
                past_date = (datetime.now() - timedelta(days=period)).strftime("%Y%m%d")
                # 과거 날짜의 OHLCV 데이터에서 종가를 가져옵니다.
                ohlcv_past = stock.get_market_ohlcv_by_date(
                    fromdate=past_date, todate=past_date, ticker=stock_code
                )
                if not ohlcv_past.empty:
                    stock_prices[f"{period}일전 값의 평균 기준"] = (
                        ohlcv_past["시가"].iloc[-1] + ohlcv_past["종가"].iloc[-1]
                    ) / 2
            sector_prices[(stock_code, stock_name)] = stock_prices
        price_data[(index_code, sector_name)] = sector_prices
    print("----", price_data)
    return price_data


def calculate_profit_loss(
    price_data, total_investment_per_sector=10000000, investment_per_stock=2000000
):
    results = {}
    for (index_code, sector_name), stocks in price_data.items():
        sector_results = []
        sector_remaining_balance = (
            total_investment_per_sector  # 섹터별 전체 투자금액 초기화
        )

        for (stock_code, stock_name), prices in stocks.items():
            current_price = prices["현재가"] if "현재가" in prices else None
            if current_price is not None:
                # 실제 투자한 주식 수 계산
                num_shares = investment_per_stock // current_price
                print("-----num_shares", num_shares)
                # 실제 투자 금액
                invested_amount = num_shares * current_price
                print("----invested_amount", invested_amount)
                # 남은 잔액 갱신
                sector_remaining_balance -= invested_amount

                stock_result = {
                    "종목코드": stock_code,
                    "종목명": stock_name,
                    "현재가": current_price,
                    "수익": {},
                }

                # 과거 평균 가격 기준으로 수익 계산
                for period_key, historical_average in prices.items():
                    if (
                        "일전 값의 평균 기준" in period_key
                        and historical_average is not None
                    ):
                        historical_value = num_shares * historical_average
                        profit_loss = num_shares * current_price - historical_value
                        stock_result["수익"][
                            period_key.replace(" 값의 평균 기준", "")
                        ] = profit_loss

                sector_results.append(stock_result)

        # 각 섹터별로 남은 잔액 추가
        sector_results.append({"Remaining Balance": sector_remaining_balance})
        results[sector_name] = sector_results

    return results
