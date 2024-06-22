# from concurrent.futures import ThreadPoolExecutor, as_completed
# import json
import time
# import warnings
from datetime import datetime, timezone
from typing import List, Union
import requests
import pandas as pd
# import okx.MarketData as MarketData
# import okx.PublicData as PublicData

__all__ = (
    'BinanceAPI',
)


class BinanceAPI:
    def __init__(self, api_key: str = '', api_secret: str = ''):
        self.api_key = api_key
        self.api_secret = api_secret
        self.limit = 500
        self.instrument_base_url = {
            'spot': 'https://api.binance.com',
            'linear_derivatives': 'https://fapi.binance.com',
            'inverse_derivatives': 'https://dapi.binance.com',
        }
        self.steps = {
            '1m': 60,
            '3m': 180,
            '5m': 300,
            '15m': 900,
            '30m': 1800,
            '1h': 3600,
            '2h': 7200,
            '4h': 14400,
            '6h': 21600,
            '8h': 28800,
            '12h': 43200,
            '1d': 86400,
            '3d': 259200,
            '1w': 604800,
        }

    def get_kline(self, symbol: str, interval: str, instrument_type: str,
                  start_date: str = '2020-01-01', end_date: Union[str, None] = None):
        if interval not in self.steps.keys():
            raise ValueError('Invalid interval')
        else:
            step = self.steps[interval]

        if end_date is None:
            end_unixts = int(datetime.now().timestamp())
        else:
            end_unixts = int(datetime.strptime(
                end_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).timestamp())

        start_unixts = int(datetime.strptime(
            start_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).timestamp())

        if instrument_type not in self.instrument_base_url.keys():
            raise ValueError('Invalid instrument_type')

        base_url = self.instrument_base_url[instrument_type]
        data = []

        if instrument_type == 'spot':
            kline_cols = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume',
                          'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
            extend_url = '/api/v3/klines'
            limit = 1000
            step = step * 1000 * limit

        elif instrument_type == 'linear_derivatives':
            # https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-data
            extend_url = '/fapi/v1/klines'
            kline_cols = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume',
                          'number_of_trades', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
            limit = 1000
            step = step * 1000 * limit

        elif instrument_type == 'inverse_derivatives':
            # https://binance-docs.github.io/apidocs/delivery/en/#kline-candlestick-data
            extend_url = '/dapi/v1/klines'
            kline_cols = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'base_asset_volume',
                          'number_of_trades', 'taker_buy_volume', 'taker_buy_base_asset_volume', 'ignore']
            limit = 200
            step = step * 1000 * limit

        else:
            raise ValueError('Invalid instrument_type')

        df = pd.DataFrame(data, columns=kline_cols)
        for t in range(start_unixts * 1000, end_unixts * 1000, step):
            try:
                start_time = t
                end_time = t + step
                if end_time > int(datetime.now().timestamp()*1000):
                    end_time = int(datetime.now().timestamp()*1000)
                params = {
                    'symbol': symbol,
                    'interval': interval,
                    'startTime': start_time,
                    'endTime': end_time,
                    'limit': limit
                }
                print(params)
                response = requests.get(base_url + extend_url, params=params)
                if response.json() == []:
                    continue
                data += response.json()
                temp = pd.DataFrame(data, columns=kline_cols)
                df = pd.concat([df, temp])
                time.sleep(2)
            except Exception as e:
                print(e.__str__())
                print(f'Error: {symbol} {interval} {instrument_type}')
                time.sleep(2)
                continue
        duplicates = df.duplicated(subset=['open_time'])
        df = df[~duplicates]
        df = df.reset_index(drop=True)
        df['open_time'] = df['open_time'].astype(float)
        df.sort_values(by='open_time', ascending=True, inplace=True)
        df['date_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df.set_index('date_time', inplace=True)
        return df

    def get_fundingrate(self, symbol: str, interval: str, instrument_type: str,
                        start_date: str = '2020-06-01', end_date: Union[str, None] = None):
        interval = '8h'
        if interval not in self.steps.keys():
            raise ValueError('Invalid interval')
        else:
            step = self.steps[interval]

        if end_date is None:
            end_unixts = int(datetime.utcnow().timestamp())
        else:
            end_unixts = int(datetime.strptime(
                end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())

        start_unixts = int(datetime.strptime(
            start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())

        if instrument_type not in self.instrument_base_url.keys():
            raise ValueError('Invalid instrument_type')

        base_url = self.instrument_base_url[instrument_type]
        data = []

        if instrument_type == 'linear_derivatives':
            # https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-data
            extend_url = '/fapi/v1/fundingRate'
            kline_cols = ['symbol', 'fundingTime', 'fundingRate', 'markPrice']
            limit = 500
            step = step * 1000 * limit

        elif instrument_type == 'inverse_derivatives':
            # https://binance-docs.github.io/apidocs/delivery/en/#kline-candlestick-data
            extend_url = '/dapi/v1/fundingRate'
            kline_cols = ['symbol', 'fundingTime', 'fundingRate', 'markPrice']
            limit = 500
            step = step * 1000 * limit
        else:
            raise ValueError('Invalid instrument_type')
        df = pd.DataFrame(data, columns=kline_cols)
        for t in range(start_unixts * 1000, end_unixts * 1000, step):
            if t+step > int(datetime.utcnow().timestamp()*1000):
                t = int(datetime.utcnow().timestamp()*1000) - step
            try:
                start_time = t
                end_time = t + step
                params = {
                    'symbol': symbol,
                    'startTime': start_time,
                    'endTime': end_time,
                    'limit': limit  # Maximum limit
                }
                response = requests.get(base_url + extend_url, params=params)
                if response.json() == []:
                    continue
                data += response.json()
                temp = pd.DataFrame(data, columns=kline_cols)
                df = pd.concat([df, temp])
                time.sleep(2)
            except Exception as e:
                print(e.__str__())
                print(f'Error: {symbol} {interval} {instrument_type}')
                time.sleep(2)
                continue
        duplicates = df.duplicated(subset=['fundingTime'])
        df = df[~duplicates]
        df = df.reset_index(drop=True)
        df['date'] = df['fundingTime'].astype(float)
        df.sort_values(by='fundingTime', ascending=True, inplace=True)
        df['date'] = pd.to_datetime(df['date'], unit='ms')
        df.set_index('date', inplace=True)
        return df


def download_full_binance_data(token, path):
    bn = BinanceAPI('', '')
    intevals = ['1d', '4h', '1h', '15m', '5m', '1m']

    for interval in intevals:
        for ins in ['inverse_derivatives']:
            try:
                print(f'Downloading {token} {interval} {ins}')
                if ins == 'spot':
                    kline = bn.get_kline(
                        symbol=f'{token}USDT', interval=interval,
                        instrument_type='spot', start_date='2020-06-01'
                    )
                elif ins == 'linear_derivatives':
                    kline = bn.get_kline(
                        symbol=f'{token}USDT', interval=interval,
                        instrument_type='linear_derivatives', start_date='2020-06-01'
                    )
                else:
                    kline = bn.get_kline(
                        symbol=f'{token}USD_PERP', interval=interval,
                        instrument_type='inverse_derivatives', start_date='2020-06-01'
                    )
                kline.to_parquet(
                    fr'{path}/{token}_{interval}_binance_{ins}.parquet')
            except Exception as e:
                print(e.__str__())
                print(f'Error: {token} {interval} {ins}')
                time.sleep(2)
                continue


def download_full_binance_fundingrate(token, path):
    bn = BinanceAPI('', '')
    intevals = ['8h']

    for interval in intevals:
        for ins in ['inverse_derivatives']:
            try:
                print(f'Downloading {token} {interval} {ins}')
                if ins == 'linear_derivatives':
                    kline = bn.get_fundingrate(
                        symbol=f'{token}USDT', interval=interval,
                        instrument_type='linear_derivatives', start_date='2020-06-01'
                    )
                elif ins == 'inverse_derivatives':
                    kline = bn.get_fundingrate(
                        symbol=f'{token}USD_PERP', interval=interval,
                        instrument_type='inverse_derivatives', start_date='2020-06-01'
                    )
                else:
                    print('Invalid Instrument Type')
                kline.to_parquet(
                    fr'{path}/{token}_{interval}_binance_{ins}_fundingrate.parquet')
            except Exception as e:
                print(e.__str__())
                print(f'Error: {token} {interval} {ins}')
                time.sleep(2)
                continue
