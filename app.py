import streamlit as st
import pandas as pd
import time

from utils import *


def visualize_bybit_gates_orderbook(token: str):
    bybit_bids = get_bybit_pre_market_orderbook(token, 'buy')
    bybit_asks = get_bybit_pre_market_orderbook(token, 'sell')
    gateio_bids = get_gateio_pre_market_orderbook(token, 'buy')
    gateio_asks = get_gateio_pre_market_orderbook(token, 'sell')
    agg_bids = convert_orderbook_tick(
        bybit_bids, 'bybit') + convert_orderbook_tick(gateio_bids, 'gateio')
    agg_asks = convert_orderbook_tick(
        bybit_asks, 'bybit') + convert_orderbook_tick(gateio_asks, 'gateio')
    agg_bids_df = pd.DataFrame(agg_bids)
    agg_asks_df = pd.DataFrame(agg_asks)
    agg_bids_df.sort_values(
        by=['price'], ascending=False, inplace=True, ignore_index=True)
    agg_asks_df.sort_values(
        by=['price'], ascending=True, inplace=True, ignore_index=True)
    st.subheader(f'Aggregated Orderbooks ({token}):')
    st.write(pd.concat([
        agg_bids_df.rename(
            columns={c: f'{c}_bid' for c in agg_asks_df.columns}),
        agg_asks_df.rename(columns={c: f'{c}_ask' for c in agg_asks_df.columns})],
        axis=1)[['exchange_bid', 'order_amount_bid', 'quantity_bid', 'price_bid', 'price_ask', 'quantity_ask', 'order_amount_ask', 'exchange_ask']])


def visualize_whales_bybit_orderbook(token: str):

    # common_tokens
    # token = 'BLAST'
    whales_bids = get_whales_pre_market_orderbook(token, 'buy')
    whales_asks = get_whales_pre_market_orderbook(token, 'sell')
    whales_bids = whales_bids[whales_bids['filled_amount']
                              <= 0.9*whales_bids['total_amount']]
    whales_asks = whales_asks[whales_asks['filled_amount']
                              <= 0.9*whales_asks['total_amount']]

    bybit_bids = get_bybit_pre_market_orderbook(token, 'buy')
    bybit_asks = get_bybit_pre_market_orderbook(token, 'sell')
    bybit_bids['price'] = [float(p) for p in bybit_bids['price']]
    bybit_asks['price'] = [float(p) for p in bybit_asks['price']]
    bybit_bids['quantity'] = [float(p) for p in bybit_bids['quantity']]
    bybit_asks['quantity'] = [float(p) for p in bybit_asks['quantity']]

    agg_bids = convert_orderbook_tick(
        bybit_bids, 'bybit', .98) + convert_orderbook_tick(whales_bids, 'whales', .975)
    agg_asks = convert_orderbook_tick(
        bybit_asks, 'bybit', 1.02) + convert_orderbook_tick(whales_asks, 'whales', 1/.975)
    # agg_bids
    agg_bids_df = pd.DataFrame(agg_bids)
    agg_asks_df = pd.DataFrame(agg_asks)
    agg_bids_df['collateral_quoted_in'] = agg_bids_df['collateral_quoted_in'].fillna(
        "USDT")
    agg_asks_df['collateral_quoted_in'] = agg_asks_df['collateral_quoted_in'].fillna(
        "USDT")

    agg_bids_df.sort_values(by=['price'], ascending=False,
                            inplace=True, ignore_index=True)
    agg_asks_df.sort_values(by=['price'], ascending=True,
                            inplace=True, ignore_index=True)
    orb = pd.concat([
        agg_bids_df.rename(
            columns={c: f'{c}_b' for c in agg_asks_df.columns}),
        agg_asks_df.rename(columns={c: f'{c}_a' for c in agg_asks_df.columns})],
        axis=1)
    st.subheader(f'Aggregated Orderbooks ({token}): (Whales & Bybit)')
    st.subheader('bid - ask =' + str(orb['price_b'][0] - orb['price_a'][0]))
    # st.write(orb)
    st.write(orb[['exch_b', 'spread_b', 'raw_price_b', 'collateral_quoted_in_b', 'order_amt_b', 'unfilled_qty_b', 'price_b',
                 'price_a', 'unfilled_qty_a', 'order_amt_a', 'collateral_quoted_in_a', 'raw_price_a', 'spread_a', 'exch_a']])


if __name__ == '__main__':
    curr_time = time.time()*1000
    whales_tokens = get_whales_pre_market_currencies()
    bybit_tokens = get_bybit_pre_market_currencies()
    bybit_tokens = bybit_tokens[(
        bybit_tokens['tradeEndTime'] > curr_time) | (bybit_tokens['tradeEndTime'] == 0)]
    # gateio_tokens = get_gateio_pre_market_currencies()
    common_tokens = list(set(bybit_tokens['tokenName'].unique().tolist()) & set(
        whales_tokens['symbol'].unique().tolist()))
#  & set(gateio_tokens['currency'].unique().tolist())
    # Add a selection button
    # selected_token = st.selectbox('Select Token', common_tokens)

    # Visualize the selected token
    for token in common_tokens:
        visualize_whales_bybit_orderbook(token)
        st.subheader("")

    st.subheader(f'Whales Pre-market Tokens:')
    st.write(whales_tokens)
    st.subheader(f'Bybit Pre-market Tokens:')
    st.write(bybit_tokens)
