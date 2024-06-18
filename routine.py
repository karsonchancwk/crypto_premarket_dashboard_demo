import requests
import pandas as pd
import time
import math
from utils import *

from tg import TGSuperGroupBot
tgs = TGSuperGroupBot(token='7285449724:AAF9uaJ7kD1pI05Dw-uZ5rsMsOIGjhIzh04',
                      chat_id='-1002215257707', message_thread_id=2)

orb = None


def fetch_orb(token):
    global orb

    # token = common_tokens[0]
    # token = 'ZRO'

    # fetch and clean orderbooks
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

    # Combine orderbooks of bid and ask from 2 exchagnes
    agg_bids = convert_orderbook_tick(
        bybit_bids, 'bybit', .98) + convert_orderbook_tick(whales_bids, 'whales', .975)
    agg_asks = convert_orderbook_tick(
        bybit_asks, 'bybit', 1.02) + convert_orderbook_tick(whales_asks, 'whales', 1/.975)
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
        agg_bids_df.rename(columns={c: f'{c}_b' for c in agg_asks_df.columns}),
        agg_asks_df.rename(columns={c: f'{c}_a' for c in agg_asks_df.columns})],
        axis=1)


def take_arb(token):
    global orb
    bids = orb[['exch_b', 'spread_b', 'raw_price_b', 'collateral_quoted_in_b',
                'order_amt_b', 'qty_b', 'price_b']].dropna(how='all')
    asks = orb[['price_a', 'qty_a', 'order_amt_a', 'collateral_quoted_in_a',
                'raw_price_a', 'spread_a', 'exch_a']].dropna(how='all')
    row_b = row_a = 0
    max_row_b = len(bids.index)
    max_row_a = len(asks.index)

    # token = 'EIGEN'#!!!!!!!!!!!!!!!!!!!!!!
    qty_profits = pd.DataFrame(
        columns=['row_b', 'row_a', 'total_qty', 'total_profit'])
    total_qty = 0.0
    total_profit = 0.0
    bids['to_be_taken'] = [False for _ in range(max_row_b)]
    asks['to_be_taken'] = [False for _ in range(max_row_a)]

    # Take each order row by row until price_a > price_b
    while row_b < max_row_b and row_a < max_row_a:
        print(token, (row_b, row_a))

        # Calculate the bid-ask spread
        price_diff = bids['price_b'][row_b] - asks['price_a'][row_a]
        if price_diff <= 0:
            break
        print('price_diff', price_diff)

        # Determine whether there are marginal gas required from entering into contract
        marginal_gas = 0
        if bids['to_be_taken'][row_b] == False:
            marginal_gas += network_name_to_gas(fetch_network_name(
                token)) if bids['exch_b'][row_b] == 'whales' else 0
        if asks['to_be_taken'][row_a] == False:
            marginal_gas += network_name_to_gas(fetch_network_name(
                token)) if asks['exch_a'][row_a] == 'whales' else 0
        print('marginal_gas', marginal_gas)

        # Determine the marginal qty after considering one more bid/ask order
        marginal_qty = 0
        if bids['to_be_taken'][row_b] == False and asks['to_be_taken'][row_a] == False:
            marginal_qty = min(bids['qty_b'][row_b], asks['qty_a'][row_a])
        elif bids['to_be_taken'][row_b] == False:
            marginal_qty = min(sum(bids['qty_b'][:row_b+1]),
                               sum(asks['qty_a'][:row_a+1])) - total_qty
        else:
            marginal_qty = min(sum(asks['qty_a'][:row_a+1]),
                               sum(bids['qty_b'][:row_b+1])) - total_qty
        print('marginal_qty', marginal_qty)

        # Calculate marginal profit
        marginal_profit = marginal_qty * price_diff - marginal_gas
        print('marginal_profit', marginal_profit)

        # Update looping parameters
        total_qty += marginal_qty
        total_profit += marginal_profit
        qty_profits.loc[len(qty_profits.index)] = [
            row_b, row_a, total_qty, total_profit]
        bids.loc[row_b, 'to_be_taken'] = True
        asks.loc[row_a, 'to_be_taken'] = True

        if math.isclose(sum(bids['qty_b'][:row_b+1]), total_qty):
            row_b += 1
        if math.isclose(sum(asks['qty_a'][:row_a+1]), total_qty):
            row_a += 1

    # final_row_b, final_row_a, final_qty, final_profit
    final_takes = list(qty_profits[qty_profits['total_profit'] ==
                                   qty_profits['total_profit'].max()].to_dict('index').values())[0]
    if final_takes['total_profit'] < 0.0:
        print(token, 'no profit')
        return
    final_takes['row_b'] = int(final_takes['row_b'])
    final_takes['row_a'] = int(final_takes['row_a'])

    # Calculate the take percentage of each bid order and the collateral required
    remaining_qty = final_takes['total_qty']
    total_collateral = 0
    for i in range(max_row_b):
        if math.isclose(remaining_qty, 0.0):
            break
        if remaining_qty < bids['qty_b'][i]:
            total_collateral += remaining_qty * bids['price_b'][i]
            bids.loc[i, 'take_percent'] = remaining_qty / bids['qty_b'][i]
            break
        total_collateral += bids['order_amt_b'][i]
        remaining_qty -= bids['qty_b'][i]
        bids.loc[i, 'take_percent'] = 1.0

    # Calculate the take percentage of each ask order
    remaining_qty = final_takes['total_qty']
    for i in range(max_row_a):
        if math.isclose(remaining_qty, 0.0):
            break
        if remaining_qty < asks['qty_a'][i]:
            asks.loc[i, 'take_percent'] = remaining_qty / asks['qty_a'][i]
            break
        remaining_qty -= asks['qty_a'][i]
        asks.loc[i, 'take_percent'] = 1.0

    buy_msg = ""
    for i in range(final_takes['row_a']+1):
        buy_msg += f"price={asks['price_a'][i]},qty={asks['qty_a'][i]},exchange={asks['exch_a'][i]},take_percent={asks['take_percent'][i]}; "
    sell_msg = ""
    for i in range(final_takes['row_b']+1):
        sell_msg += f"price={bids['price_b'][i]},qty={bids['qty_b'][i]},collateral={bids['order_amt_b'][i]}{bids['collateral_quoted_in_b'][i]},exchange={bids['exch_b'][i]},take_percent={bids['take_percent'][i]}; "

    tgs.send_message(f"""
        Token: {token}
        Buy: {buy_msg}
        Sell:{sell_msg}
        Total_qty: {total_qty} {token}
        Total_profit: {total_profit} USDT
        Collateral_required: {total_collateral} USDT
        Turnover percent= {total_profit/total_collateral}
        """)


def run_per_minute():
    global orb
    tokens = get_available_currencies()
    for token in tokens:
        fetch_orb(token)
        # if token == tokens[-1]:
        #     orb = pd.read_csv('./EIGEN_orderbook_testdata.csv')
        price_diff = orb['price_b'][0] - orb['price_a'][0]
        if price_diff < 0:
            print(token, 'bid < ask')
            continue
        take_arb(token)


def run_per_hour():
    tgs.send_message('The bot is running properly for the previous hour')


if __name__ == '__main__':
    # For testing purpose, pls uncomment orb = pd.read_csv('./EIGEN_orderbook_testdata.csv')
    run_per_minute()
    run_per_hour()
