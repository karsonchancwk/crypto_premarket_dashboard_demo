import requests
import pandas as pd
import time
import math


def get_whales_pre_market_currencies() -> pd.DataFrame:
    # def get_whales_pre_market_currencies() -> list[str]:
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'if-none-match': 'W/"5f0d-8HSEDhAaRqqgDuK6HrpWGMyHKbk"',
        'origin': 'https://pro.whales.market',
        'priority': 'u=1, i',
        'referer': 'https://pro.whales.market/',
        'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    }

    response = requests.get(
        'https://api-v2.whales.market/v2/tokens?take=100&page=1&type=pre_market&search=&category=pre_market&statuses[]=active&statuses[]=settling&sort_vol=desc',
        headers=headers,)

    if response.status_code != 200:
        raise Exception('Request Error')

    j = response.json()
    # print(j)
    return pd.DataFrame(j['data']['list'])
    # return [curr['symbol'] for curr in j['data']['list']]


def get_gateio_pre_market_currencies() -> pd.DataFrame:
    gateio_premarket_currencies_url = 'https://www.gate.io/apiw/v2/pre_market/currencies?type=1&page=1&limit=99'
    response = requests.get(gateio_premarket_currencies_url)

    if response.status_code != 200:
        raise Exception('Request Error')

    j = response.json()
    df = pd.DataFrame(j['data']['list'])

    return df


def get_bybit_pre_market_currencies() -> pd.DataFrame:
    cookies = {
        '_abck': 'B796D0364EFB4118DD46CE1BC906E381~0~YAAQxetGaHUqlVGPAQAAM+9Hjwua4sJdo6LUgbw1O0Gy9ugKCe/I6fxmK/bIR9Tz/vaCmXiBOewvfn1iMf8kUenelsjR498rwkFRYjIkWcq3Q7WzGClNzvrdJ7fUguDSaFqj5/7cSLhY0mqzJ2Qo4eJgh1GCmyOflUfDKXSsEVdBlsQK5tW2xrrKZPH7uxdws28CVgGCh99lahAFFf6HO3eJEvn1ywnFtDuJ2TMzpjnwqcOVtIGNDoLcQTSKOk30JZHYoSXupwBk6HE9i+QS2YGxq9ryXbdW59gEOSREBge94TvFWs3rsUmW7fwDuhOP4AQqW7ysAGN0HS2Az5Uqh1JIsVdA/hZQrhGEWb6Ff035xEpg8BWdh0mPRsti~-1~-1~-1',
    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en',
        'dnt': '1',
        'lang': 'en',
        'origin': 'https://www.bybit.com',
        'priority': 'u=1, i',
        'referer': 'https://www.bybit.com/',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'traceparent': '00-f355bb1ea82b8685509918c86a6729db-e548f6630a12a96e-00',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }

    response = requests.get(
        'https://api2.bybit.com/spot/api/marketTrading/v1/token/tokenList', cookies=cookies, headers=headers)

    if response.status_code != 200:
        raise Exception('Request Error')

    j = response.json()
    df = pd.DataFrame(j['result'])

    return df


def get_whales_pre_market_orderbook(token: str, side: str) -> pd.DataFrame:
    if side != 'buy' and side != 'sell':
        raise Exception('side should be either buy or sell')

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'if-none-match': 'W/"1eb3-hannLXcIpqHTs8gMYRF9uJAYOQE"',
        'origin': 'https://pro.whales.market',
        'priority': 'u=1, i',
        'referer': 'https://pro.whales.market/',
        'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    }

    params = {
        'take': '30',
        'page': '1',
        'type': side,
        'full_match': '',
        'symbol': token,
        'status': 'open',
        'min_price': '',
        'max_price': '',
        'sort_price': 'ASC',
        'category_token': 'pre_market',
        # 'chains': '666666',
    }

    response = requests.get(
        'https://api-v2.whales.market/v2/offers', params=params, headers=headers)

    if response.status_code != 200:
        raise Exception('Error')

    j = response.json()
    # print(j['data'])
    # obs =

    return pd.DataFrame(j['data']['list'])


def get_bybit_pre_market_orderbook(token: str, side: str) -> pd.DataFrame:
    if side == 'sell':
        order_type = '1'

    if side == 'buy':
        order_type = '2'

    cookies = {
        '_abck': 'B796D0364EFB4118DD46CE1BC906E381~0~YAAQhOtGaOkusU+PAQAA2AAWjwuVOFcNCJVSiQAUh5WTv6er5VRJ5iMgdAiKcHfZVHviVJjIyd3LyWAHQ6jw82SEjJuyQIV6E0KdDet8aLbSHtZ4xGb1RWkeHzpjUXrPIJUwpVZT0QSaKjqhJ07MYj3kDzIuGKjTR7pat9zlBLmyPhS16KN789Zdjz6tzjMTz1gR2rLvU18W51TQd4bD8hk9Hj213lEstV8sCrMrj8xoIXF1RXYdTJObpEaN8RPpr8DdyAp3/fEXOnMJEE2Fgx8xQRTPYbhm7BvjvBr0C0f14oEwdcgDcEZKp65Fa95/JkMAhButrZ6CyVhB90Z4/yuGFSS92M99wlQiz8E+6efog72zvwE2ZU3lFuAPDpr0sRB5VMSBRWw=~-1~-1~-1',
    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en',
        'dnt': '1',
        'lang': 'en',
        'origin': 'https://www.bybit.com',
        'priority': 'u=1, i',
        'referer': 'https://www.bybit.com/',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'traceparent': '00-bcc60bf8778f864e0c0b82c1903f792f-cae725ec958abdce-00',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }

    params = {
        'minPrice': '',
        'maxPrice': '',
        'minAmount': '',
        'maxAmount': '',
        'showMyOrder': '0',
        'matchUserAsset': '0',
        'orderType': order_type,
        'tokenName': token,
        'pageSize': '11',
    }

    response = requests.get(
        url='https://api2.bybit.com/spot/api/marketTrading/v1/order/tradeOrders',
        params=params,
        cookies=cookies,
        headers=headers,
    )

    if response.status_code != 200:
        raise Exception('Error')

    j = response.json()
    obs = pd.DataFrame(j['result'])

    return obs


def get_gateio_pre_market_orderbook(token: str, side: str) -> pd.DataFrame:
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en,en-US;q=0.9,zh-HK;q=0.8,zh-TW;q=0.7,zh;q=0.6',
        'baggage': 'sentry-environment=production,sentry-release=2w8LYBYs_SMvIiAIlusi_,sentry-public_key=49348d5eaee2418db953db695c5c9c57,sentry-trace_id=54263241e3ae4c5cb2c8d26efe4b4440',
        'csrftoken': '1',
        'dnt': '1',
        'priority': 'u=1, i',
        'referer': f'https://www.gate.io/en/pre-market/{token}',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'sentry-trace': '54263241e3ae4c5cb2c8d26efe4b4440-9d40373b52fccc98-0',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }

    params = {
        'page': '1',
        'limit': '10',
        'currency': token,
        'side': side,
        'status': 'no_transaction',
    }

    response = requests.get(
        'https://www.gate.io/apiw/v2/pre_market/market_orders', params=params, headers=headers)

    if response.status_code != 200:
        raise Exception('Error')

    j = response.json()

    if j.get('message') == 'success':
        data_list = j['data']['list']
        obs = pd.DataFrame(data_list)
        return obs


def convert_orderbook_tick(orderbook_df: pd.DataFrame, exchange: str, spread_factor=1):
    quotes = []
    orders = orderbook_df.to_dict(orient='records')

    if exchange == 'bybit':
        for order in orders:
            quotes.append({
                'raw_price': order['price'],
                'price': order['price'] * spread_factor,
                'qty': order['quantity'],
                'unfilled_qty': order['quantity'],
                'unfilled_order_amt': order['orderAmount'],
                'order_amt': order['orderAmount'],
                'exch': 'bybit',
                'spread': spread_factor-1
            })
        return quotes
    # elif exchange == 'gateio':
    #     for order in orders:
    #         quotes.append({
    #             'price': order['price'] * spread_factor,
    #             'quantity': order['amount'],
    #             'order_amount': order['order_value'],
    #             'exchange': 'gateio',
    #         })
    #     return quotes
    elif exchange == 'whales':
        for order in orders:
            quotes.append({
                'raw_price': order['price'] * order['ex_token__price'],
                'price': order['price'] * spread_factor * order['ex_token__price'],
                'qty': order['total_amount'],
                'unfilled_qty': order['total_amount'] - order['filled_amount'],
                'order_amt': (order['total_amount'] - order['filled_amount']) * order['price'] * spread_factor * order['ex_token__price'],
                # 'total_order_amt': order['value'] * order['ex_token__price'],
                'collateral_quoted_in': order['ex_token__symbol'],
                'exch': 'whales',
                'spread': spread_factor-1
            })
        return quotes
    else:
        raise ValueError('Exchange not supported.')


def get_available_currencies() -> list[str]:
    whales_tokens = get_whales_pre_market_currencies()
    bybit_tokens = get_bybit_pre_market_currencies()

    curr_time = time.time()
    bybit_tokens = bybit_tokens[(bybit_tokens['tradeEndTime'] > curr_time) | (
        bybit_tokens['tradeEndTime'] == 0)]

    # common_tokens = list(set(gateio_tokens['currency'].unique().tolist()) & set(bybit_tokens['tokenName'].unique().tolist()))
    common_tokens = list(set(whales_tokens['symbol'].unique().tolist()) & set(
        bybit_tokens['tokenName'].unique().tolist()))
    # print('common_tokens = ', common_tokens)
    return common_tokens


def fetch_network_name(token) -> str:
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'if-none-match': 'W/"4d2-9L+LvMdAVw215IEGIl5WjHys9Z8"',
        'origin': 'https://pro.whales.market',
        'priority': 'u=1, i',
        'referer': 'https://pro.whales.market/',
        'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    }

    response = requests.get(
        f'https://api-v2.whales.market/v2/tokens/detail/{token}', headers=headers)

    if response.status_code != 200:
        raise Exception('Error')

    j = response.json()
    return j['data']['network_name']


def network_name_to_gas(network_name):
    # Each tx cost .003005 SOL gas fee (.444 U eqv)
    if network_name == 'Solana':
        fetch_sol = requests.get(
            "https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT")
        if fetch_sol.status_code != 200:
            raise Exception('Error')
        return float(fetch_sol.json()['price'])*.003005

    # Each tx cost .00076723ETH gas fee (2.75U eqv)
    elif network_name == 'Ethereum':
        fetch_eth = requests.get(
            "https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT")
        if fetch_eth.status_code != 200:
            raise Exception('Error')
        return float(fetch_eth.json()['price'])*.00076723

    # ################## SEND A TG MSG for a new chain############################################!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    return 0
