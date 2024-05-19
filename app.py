import streamlit as st
import pandas as pd
import time

from utils import *


def visualize_aggregated_orderbook(token: str):
    bybit_bids = get_bybit_pre_market_orderbook(token, 'buy')
    bybit_asks = get_bybit_pre_market_orderbook(token, 'sell')
    gateio_bids = get_gateio_pre_market_orderboook(token, 'buy')
    gateio_asks = get_gateio_pre_market_orderboook(token, 'sell')
    agg_bids = convert_orderbook_tick(bybit_bids, 'bybit') + convert_orderbook_tick(gateio_bids, 'gateio')
    agg_asks = convert_orderbook_tick(bybit_asks, 'bybit') + convert_orderbook_tick(gateio_asks, 'gateio')
    agg_bids_df = pd.DataFrame(agg_bids)
    agg_asks_df = pd.DataFrame(agg_asks)
    agg_bids_df.sort_values(by=['price'], ascending=False, inplace=True, ignore_index=True)
    agg_asks_df.sort_values(by=['price'], ascending=True, inplace=True, ignore_index=True)
    st.subheader(f'Aggregated Orderbooks ({token}):')
    st.write(pd.concat([
        agg_bids_df.rename(columns={c: f'{c}_bid' for c in agg_asks_df.columns}), 
        agg_asks_df.rename(columns={c: f'{c}_ask' for c in agg_asks_df.columns})], 
    axis=1)[['exchange_bid', 'order_amount_bid', 'quantity_bid', 'price_bid', 'price_ask', 'quantity_ask', 'order_amount_ask', 'exchange_ask']])


if __name__ == '__main__':
    bybit_currencies = get_bybit_pre_market_currencies()
    gateio_currencies = get_gateio_pre_market_currencies()
    common_tokens = list(set(bybit_currencies['tokenName'].unique().tolist()) & set(gateio_currencies['currency'].unique().tolist()))

    # Add a selection button
    selected_token = st.selectbox('Select Token', common_tokens)
    
    # Visualize the selected token
    visualize_aggregated_orderbook(selected_token)
    
    st.subheader(f'Bybit Pre-market Tokens:')
    st.write(bybit_currencies)
    st.subheader(f'GateIO Pre-market Tokens:')
    st.write(gateio_currencies)
