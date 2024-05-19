import requests 
import pandas as pd


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

    response = requests.get('https://api2.bybit.com/spot/api/marketTrading/v1/token/tokenList', cookies=cookies, headers=headers)
    
    if response.status_code != 200:
        raise Exception('Request Error')
       
    j = response.json()
    df = pd.DataFrame(j['result'])

    return df


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


def get_gateio_pre_market_orderboook(token: str, side: str) -> pd.DataFrame:
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

    response = requests.get('https://www.gate.io/apiw/v2/pre_market/market_orders', params=params, headers=headers)

    if response.status_code != 200:
        raise Exception('Error')

    j = response.json()

    if j.get('message') == 'success':
        data_list = j['data']['list']
        obs = pd.DataFrame(data_list)
        return obs
    

def convert_orderbook_tick(orderbook_df: pd.DataFrame, exchange: str, spread=0):
    quotes = []
    orders = orderbook_df.to_dict(orient='records')

    if exchange == 'bybit':
        for order in orders:
            quotes.append({
                'price': order['price'] * (1 + spread),
                'quantity': order['quantity'],
                'order_amount': order['orderAmount'],
                'exchange': 'bybit',
            })
        return quotes
    elif exchange == 'gateio':
        for order in orders:
            quotes.append({
                'price': order['price'] * (1 + spread),
                'quantity': order['amount'],
                'order_amount': order['order_value'],
                'exchange': 'gateio',
            })
        return quotes
    else:
        raise ValueError('Exchange not supported.')
