import requests
import pandas as pd

from utils import *
whales_tokens = get_whales_pre_market_currencies()
gateio_tokens = get_gateio_pre_market_currencies()
bybit_tokens = get_bybit_pre_market_currencies()
# common_tokens = list(set(gateio_tokens['currency'].unique().tolist()) & set(bybit_tokens['tokenName'].unique().tolist()))

common_tokens = list(set(whales_tokens['symbol'].unique().tolist()) & set(
    bybit_tokens['tokenName'].unique().tolist()))
# token = common_tokens[0]
token = "IO"
print(token)

whales_bids = get_whales_pre_market_orderbook(token, 'buy')
whales_asks = get_whales_pre_market_orderbook(token, 'sell')
bybit_bids = get_bybit_pre_market_orderbook(token, 'buy')
bybit_asks = get_bybit_pre_market_orderbook(token, 'sell')
gateio_bids = get_gateio_pre_market_orderbook(token, 'buy')
gateio_asks = get_gateio_pre_market_orderbook(token, 'sell')

agg_bids = convert_orderbook_tick(
    bybit_bids, 'bybit') + convert_orderbook_tick(whales_bids, 'whales')
agg_asks = convert_orderbook_tick(
    bybit_asks, 'bybit') + convert_orderbook_tick(whales_asks, 'whales')
agg_bids_df = pd.DataFrame(agg_bids)
agg_asks_df = pd.DataFrame(agg_asks)

agg_bids_df['collateral_quoted_in'] = agg_bids_df['collateral_quoted_in'].fillna(
    "USDT")
agg_asks_df['collateral_quoted_in'] = agg_asks_df['collateral_quoted_in'].fillna(
    "USDT")

print(agg_bids_df)
print()
print(agg_asks_df)

agg_bids_df.sort_values(by=['price'], ascending=False,
                        inplace=True, ignore_index=True)
agg_asks_df.sort_values(by=['price'], ascending=True,
                        inplace=True, ignore_index=True)

concated_pd = pd.concat([
    agg_bids_df.rename(columns={c: f'{c}_bid' for c in agg_asks_df.columns}),
    agg_asks_df.rename(columns={c: f'{c}_ask' for c in agg_asks_df.columns})],
    axis=1)[['exchange_bid', 'order_amount_bid', 'quantity_bid', 'price_bid', 'price_ask', 'quantity_ask', 'order_amount_ask', 'exchange_ask']]
print(concated_pd)
