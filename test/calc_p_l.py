from pykrx import stock
from datetime import datetime, timedelta
import pandas as pd
import time

def list_indices_and_stocks(market):
    """
    주어진 시장에서 지수 및 주식 목록을 가져오는 함수
    해당 코드에서는 KRX를 사용. cf) fetch_price_data()
    """

    indices = stock.get_index_ticker_list(market=market)
    index_search = {}
    for idx in indices[1:18]:
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


def fetch_price_data():
    """
    특정 기간을 설정해서 해당 일자의 각 종목 별 OHLCV 값 LOAD
    """
    indices_stocks = list_indices_and_stocks('KRX')
    periods = [7, 14, 30, 90, 180, 365]  
    today = datetime.now().strftime('%Y%m%d') 
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d') 

    price_data = {}
    for (index_code, sector_name), stocks in indices_stocks.items():
        sector_prices = {}
        # OHLCV 데이터 추출
        for stock_code, stock_name in stocks:
            ohlcv_today = stock.get_market_ohlcv_by_date(fromdate=today, todate=today, ticker=stock_code)
            ohlcv_yesterday = stock.get_market_ohlcv_by_date(fromdate=yesterday, todate=yesterday, ticker=stock_code)
            time.sleep(0.5)

            if not ohlcv_today.empty and not pd.isna(ohlcv_today['종가'].iloc[-1]):
                current_price = ohlcv_today['종가'].iloc[-1]
            elif not ohlcv_yesterday.empty:
                current_price = ohlcv_yesterday['종가'].iloc[-1]
            else:
                current_price = None 
            stock_prices = {'현재가': current_price}

            # 과거 날짜의 OHLCV 데이터에서 종가 추출
            for period in periods:
                past_date = (datetime.now() - timedelta(days=period)).strftime('%Y%m%d')
                ohlcv_past = stock.get_market_ohlcv_by_date(fromdate=past_date, todate=past_date, ticker=stock_code)
                if not ohlcv_past.empty:
                    stock_prices[f'{period}일전 값의 평균 기준'] = (ohlcv_past['시가'].iloc[-1] + ohlcv_past['종가'].iloc[-1]) / 2
            sector_prices[(stock_code, stock_name)] = stock_prices
        price_data[(index_code, sector_name)] = sector_prices
    return price_data

def calculate_profit_loss(price_data, total_investment_per_sector=10000000, investment_per_stock=2000000):
    """
    데이터 기반 손,수익 계산
    """
    results = {}
    for (index_code, sector_name), stocks in price_data.items():
        sector_results = []
        sector_remaining_balance = total_investment_per_sector

        for (stock_code, stock_name), prices in stocks.items():
            stock_result = {
                '종목코드': stock_code,
                '종목명': stock_name,
                '현재가': None,
                '수익': {},
                '투자한 주식 수': None,
                '투자 금액': None
            }

            # 과거 데이터를 기반으로 주식 수와 투자 금액 계산
            for period in ['7일전', '14일전', '30일전', '90일전', '180일전', '365일전']:
                historical_price_key = f'{period} 값의 평균 기준'
                if historical_price_key in prices and prices[historical_price_key] is not None:
                    num_shares = investment_per_stock // prices[historical_price_key]
                    invested_amount = num_shares * prices[historical_price_key]
                    sector_remaining_balance -= invested_amount

                    # 현재 가격을 가져와서 수익 계산
                    if '현재가' in prices and prices['현재가'] is not None:
                        current_value = num_shares * prices['현재가']
                        profit_loss = current_value - invested_amount
                        stock_result['수익'][period] = profit_loss
                        stock_result['현재가'] = prices['현재가']
                        stock_result['투자한 주식 수'] = num_shares
                        stock_result['투자 금액'] = invested_amount

            sector_results.append(stock_result)

        sector_results.append({'Remaining Balance': sector_remaining_balance})
        results[sector_name] = sector_results

    return results


