import pandas as pd
import numpy as np
import pandas_ta as ta
import yfinance as yf
import os
import warnings
import time
from datetime import datetime, timedelta
import pytz

warnings.filterwarnings('ignore')

# --- CONFIGURATION ---
FILE_1_ALLOCATIONS = "daily_allocations.csv"
FILE_2_PORTFOLIO = "historical_portfolio_value.csv"
FILE_3_TRADES = "strategy_trade_history.csv"
TICKER_FILE = "ind_nifty500list.csv"

MAX_POSITIONS = 6
INITIAL_CAPITAL = 1000000.0

ist = pytz.timezone('Asia/Kolkata')
now_ist = datetime.now(ist)

# --- TIME-BASED PHASE DETECTION ---
# Execution Phase: 9:30 AM to 3:40 PM (No decisions, only updates)
# Decision Phase: 3:40 PM to 9:30 AM (Generates new Buy/Sell signals)
current_minutes = now_ist.hour * 60 + now_ist.minute
market_open_minutes = 9 * 60 + 30   # 9:30 AM (570 minutes)
market_close_minutes = 15 * 60 + 40 # 3:40 PM (940 minutes)

is_decision_phase = not (market_open_minutes <= current_minutes < market_close_minutes)
phase_name = "DECISION PHASE (Generating Signals)" if is_decision_phase else "EXECUTION PHASE (Fulfilling Trades)"

print(f"Running Standalone EOD Strategy -> {phase_name}. Time (IST): {now_ist.strftime('%Y-%m-%d %H:%M:%S')}")

# ==========================================
# 1. LOAD TICKERS & FETCH YFINANCE DATA
# ==========================================
try:
    ticker_df = pd.read_csv(TICKER_FILE)
    if 'Symbol' in ticker_df.columns:
        raw_symbols = ticker_df['Symbol'].tolist()
    elif 'Ticker' in ticker_df.columns:
        raw_symbols = ticker_df['Ticker'].tolist()
    else:
        raw_symbols = ticker_df.iloc[:, 0].tolist()
        
    nifty_tickers = [str(sym).strip() + '.NS' for sym in raw_symbols if pd.notna(sym)]
except FileNotFoundError:
    print(f"Warning: {TICKER_FILE} not found. Using a fallback list.")
    nifty_tickers = ["RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS", "SBI.NS"]

print(f"Downloading historical data for {len(nifty_tickers)} tickers from YFinance...")
raw_data = yf.download(nifty_tickers, start="2020-01-01", progress=False, auto_adjust=False, threads=False)

df = raw_data.stack(level=1, future_stack=True).reset_index()
df.rename(columns={'Date': 'DATE', 'Ticker': 'TICKER', 'Open': 'OPEN', 'High': 'HIGH', 'Low': 'LOW', 'Close': 'CLOSE'}, inplace=True)
df['DATE'] = pd.to_datetime(df['DATE']).dt.tz_localize(None)
df.dropna(subset=['CLOSE'], inplace=True)

# Fetch Index Data for Regime Filter & Benchmark
print("Downloading Nifty 500 Index data...")

# Fetch Nifty 500 Index data
nifty500_data = yf.download("^CRSLDX", start="2020-01-01", progress=False, auto_adjust=False)
if nifty500_data.empty:
    print("^CRSLDX not found, trying ^NSEI as fallback for Nifty 500...")
    nifty500_data = yf.download("^NSEI", start="2020-01-01", progress=False, auto_adjust=False)

if isinstance(nifty500_data.columns, pd.MultiIndex):
    nifty500_data.columns = nifty500_data.columns.droplevel(1)

nifty500_data = nifty500_data.reset_index()
nifty500_data.rename(columns={'Date': 'DATE', 'High': 'HIGH', 'Low': 'LOW', 'Close': 'CLOSE'}, inplace=True)
nifty500_data['DATE'] = pd.to_datetime(nifty500_data['DATE']).dt.tz_localize(None)

# Calculate Supertrend for Nifty 500 (for regime filter)
st_idx = ta.supertrend(high=nifty500_data['HIGH'], low=nifty500_data['LOW'], close=nifty500_data['CLOSE'], length=15, multiplier=2.75)
nifty500_data['Index_ST_DIR'] = st_idx.iloc[:, 1].map({1: 'Up', -1: 'Down'}) if st_idx is not None else 'Up'
index_regime = nifty500_data[['DATE', 'Index_ST_DIR']].dropna()

# ==========================================
# 2. CALCULATE INDICATORS
# ==========================================
print("Calculating Strategy Indicators...")
df = df.sort_values(by=['TICKER', 'DATE']).reset_index(drop=True)

df['1D_Return'] = df.groupby('TICKER')['CLOSE'].pct_change(1)
df['1M_Return'] = df.groupby('TICKER')['CLOSE'].pct_change(21)
df['3M_Return'] = df.groupby('TICKER')['CLOSE'].pct_change(63)
df['6M_Return'] = df.groupby('TICKER')['CLOSE'].pct_change(126)
df['9M_Return'] = df.groupby('TICKER')['CLOSE'].pct_change(189)
df['12M_Return'] = df.groupby('TICKER')['CLOSE'].pct_change(252)

df['SMA_50'] = df.groupby('TICKER')['CLOSE'].transform(lambda x: ta.sma(x, length=50))
df['SMA_200'] = df.groupby('TICKER')['CLOSE'].transform(lambda x: ta.sma(x, length=200))
df['EMA_9'] = df.groupby('TICKER')['CLOSE'].transform(lambda x: ta.ema(x, length=9))
df['RSI_14'] = df.groupby('TICKER')['CLOSE'].transform(lambda x: ta.rsi(x, length=14))

st_list = []
for ticker, group in df.groupby('TICKER'):
    st = ta.supertrend(high=group['HIGH'], low=group['LOW'], close=group['CLOSE'], length=15, multiplier=2.75)
    st_res = pd.DataFrame({'ST_15_3': st.iloc[:, 0], 'ST_DIR': st.iloc[:, 1].map({1: 'Up', -1: 'Down'})} if st is not None else {'ST_15_3': np.nan, 'ST_DIR': np.nan}, index=group.index)
    st_list.append(st_res)

st_df = pd.concat(st_list)
df['ST_15_3'] = st_df['ST_15_3']
df['ST_DIR'] = st_df['ST_DIR']

df['Weighted Avg'] = (0.40 * df['3M_Return'].fillna(0)) + \
                     (0.20 * df['6M_Return'].fillna(0)) + \
                     (0.20 * df['9M_Return'].fillna(0)) + \
                     (0.20 * df['12M_Return'].fillna(0))

df['RS'] = df.groupby('DATE')['Weighted Avg'].rank(pct=True) * 100
df['RS'] = df['RS'].round(0).clip(lower=1, upper=99)

df_bt = df.dropna(subset=['SMA_200', 'RS', 'ST_15_3']).copy()

df_bt = pd.merge(df_bt, index_regime, on='DATE', how='left')
df_bt['Index_ST_DIR'] = df_bt['Index_ST_DIR'].ffill().fillna('Up')

# Re-sort to ensure chronologically safe shifting
df_bt = df_bt.sort_values(by=['TICKER', 'DATE']).reset_index(drop=True)

# ==========================================
# 3. SIGNAL LOGIC & T+1 SHIFTING
# ==========================================
is_not_circuit = df_bt['HIGH'] != df_bt['LOW']
is_index_down = df_bt['Index_ST_DIR'] == 'Down'

rsi_threshold = 55
return_1d_threshold = -0.05
rs_score_threshold = np.where(is_index_down, 95, 80)

stock_selection = ((df_bt['RSI_14'] >= rsi_threshold) & (df_bt['1D_Return'] > return_1d_threshold) &  
                   ((df_bt['3M_Return'] > 0.20) | (df_bt['6M_Return'] > 0.30) | (df_bt['1M_Return'] > 0.10)) &  
                   (df_bt['SMA_50'] > df_bt['SMA_200']))

buy_condition = ((df_bt['RS'] > rs_score_threshold) & (df_bt['CLOSE'] > df_bt['SMA_200']) &  
                 (df_bt['CLOSE'] > df_bt['ST_15_3']) & (df_bt['CLOSE'] >= df_bt['EMA_9'] * 0.95) &  
                 (df_bt['CLOSE'] <= df_bt['EMA_9'] * 1.05))

df_bt['Buy_Signal'] = is_not_circuit & stock_selection & buy_condition

# SHIFTING: We evaluate yesterday to act today
shift_cols = ['Buy_Signal', 'CLOSE', 'ST_15_3', 'SMA_200', 'RS', 'RSI_14', '3M_Return', '6M_Return', '9M_Return', 'ST_DIR', 'Index_ST_DIR']
for col in shift_cols:
    df_bt[f'Prev_{col}'] = df_bt.groupby('TICKER')[col].shift(1)

df_bt['Prev_Buy_Signal'] = df_bt['Prev_Buy_Signal'].fillna(False).astype(bool)

print("Filtering data to start portfolio execution strictly from 2021-01-01...")
df_bt = df_bt[df_bt['DATE'] >= '2021-01-01']

# ==========================================
# 4. BACKTEST ENGINE (T+1 EXECUTION)
# ==========================================
print("Running Backtest Engine (T+1 Next-Day Open Execution)...")
capital = INITIAL_CAPITAL
positions = {}
trades = []
equity_curve = []
total_brokerage = 0.0

unique_dates = sorted(df_bt['DATE'].unique())

POSITION_SIZE = INITIAL_CAPITAL / MAX_POSITIONS
BROKERAGE_RATE = 0.005

for current_date in unique_dates:
    daily_data = df_bt[df_bt['DATE'] == current_date].set_index('TICKER')
    tickers_to_remove = []
    
    # Process Sells
    for ticker, pos in positions.items():
        if ticker in daily_data.index:
            row = daily_data.loc[ticker]
            today_open, today_low, today_high = row['OPEN'], row['LOW'], row['HIGH']
            
            prev_close = row['Prev_CLOSE']
            prev_st = row['Prev_ST_15_3']
            prev_sma = row['Prev_SMA_200']
            prev_index_st = row['Prev_Index_ST_DIR']
            
            if today_high == today_low: continue

            triggered_sell = False
            sell_reason = ""
            exit_price = 0

            # 1. EOD Indicator Sells
            if pd.notna(prev_close) and pd.notna(prev_st) and prev_close < prev_st:
                exit_price = today_open
                sell_reason = "Close < ST(15,3) (T-1)"
                triggered_sell = True
            elif pd.notna(prev_close) and pd.notna(prev_sma) and prev_close < prev_sma:
                exit_price = today_open
                sell_reason = "Close < 200 SMA (T-1)"
                triggered_sell = True

            # 2. Intraday Stoploss
            if not triggered_sell:
                sl_multiplier = 0.85 if prev_index_st == 'Down' else 0.87
                sl_price = pos['raw_entry_price'] * sl_multiplier

                if today_open <= sl_price:
                    exit_price = today_open
                    sell_reason = f"SL Gap Down ({int((1-sl_multiplier)*100)}%)"
                    triggered_sell = True
                elif today_low <= sl_price:
                    exit_price = sl_price
                    sell_reason = f"Stoploss {int((1-sl_multiplier)*100)}%"
                    triggered_sell = True

            if triggered_sell:
                net_exit_price = exit_price * (1 - BROKERAGE_RATE)
                total_brokerage += exit_price * pos['qty'] * BROKERAGE_RATE
                ret_pct = (net_exit_price / pos['net_entry_price'] - 1) * 100
                pnl = (net_exit_price - pos['net_entry_price']) * pos['qty']
                capital += (net_exit_price * pos['qty'])

                trades.append({
                    'Ticker': ticker, 'Buy Price': round(pos['net_entry_price'], 2), 'Quantity': pos['qty'],
                    'Buy Date': pos['entry_date'], 'Sell Price': round(net_exit_price, 2), 'Sell Date': current_date,
                    'Sell Reason': sell_reason, 'RSI': pos['rsi'], '3M Return': pos['ret_3m'], '6M Return': pos['ret_6m'],
                    '9M Return': pos['ret_9m'], 'RS': pos['rs'], 'ST Value': pos['st_val'], 'ST Dir': pos['st_dir'],
                    'Return %': round(ret_pct, 2), 'PnL ₹': round(pnl, 2), 'Holding Days': (current_date - pd.to_datetime(pos['entry_date'])).days
                })
                tickers_to_remove.append(ticker)

    for t in tickers_to_remove: del positions[t]

    # Process Buys
    buy_signals = daily_data[daily_data['Prev_Buy_Signal']].sort_values(by='Prev_RS', ascending=False)

    for ticker, row in buy_signals.iterrows():
        if row['HIGH'] == row['LOW']: continue
        if len(positions) < MAX_POSITIONS and ticker not in positions:
            
            invest_amount = POSITION_SIZE if capital >= POSITION_SIZE else capital
            
            # Fallback for yfinance live intraday Open missing data glitch
            execution_price = row['OPEN'] 
            if pd.isna(execution_price) or execution_price == 0:
                execution_price = row['CLOSE']
                
            net_buy_price = execution_price * (1 + BROKERAGE_RATE)

            if invest_amount > net_buy_price:
                qty = int(invest_amount // net_buy_price)
                cost = qty * net_buy_price
                total_brokerage += execution_price * qty * BROKERAGE_RATE
                positions[ticker] = {
                    'raw_entry_price': execution_price, 'net_entry_price': net_buy_price, 'qty': qty, 
                    'entry_date': current_date, 'index_st': row['Index_ST_DIR'], 
                    'is_new': (current_date.date() == now_ist.date()), # Flags as new only if executed TODAY
                    'rsi': round(row['Prev_RSI_14'], 2) if pd.notna(row['Prev_RSI_14']) else 0,
                    'ret_3m': round(row['Prev_3M_Return'] * 100, 2) if pd.notna(row['Prev_3M_Return']) else 0,
                    'ret_6m': round(row['Prev_6M_Return'] * 100, 2) if pd.notna(row['Prev_6M_Return']) else 0,
                    'ret_9m': round(row['Prev_9M_Return'] * 100, 2) if pd.notna(row['Prev_9M_Return']) else 0,
                    'rs': row['Prev_RS'], 'st_val': round(row['Prev_ST_15_3'], 2) if pd.notna(row['Prev_ST_15_3']) else 0,
                    'st_dir': row['Prev_ST_DIR']
                }
                capital -= cost

    # Record Daily Equity
    daily_portfolio_value = capital
    for ticker, pos in positions.items():
        if ticker in daily_data.index:
            daily_portfolio_value += pos['qty'] * daily_data.loc[ticker, 'CLOSE']
        else:
            daily_portfolio_value += pos['qty'] * pos['raw_entry_price']
    equity_curve.append({'DATE': current_date, 'Equity': daily_portfolio_value})

# ==========================================
# 5. PREPARE TODAY'S TARGETS (DASHBOARD OUTPUT)
# ==========================================
print("Preparing Output Files...")
latest_date = unique_dates[-1]

recent_cutoff = latest_date - pd.Timedelta(days=5)
last_day_data = df_bt[df_bt['DATE'] >= recent_cutoff].groupby('TICKER').tail(1).set_index('TICKER')

latest_equity = equity_curve[-1]['Equity'] if equity_curve else INITIAL_CAPITAL

sells_for_tomorrow = []
sell_rows_for_export = [] 

# ONLY generate new decisions if running after 3:40 PM or before 9:30 AM (Decision Phase)
if is_decision_phase:
    # Check for Sells based on TODAY'S closing indicators
    for ticker, pos in list(positions.items()): # list() protects against dictionary resizing
        if ticker in last_day_data.index:
            row = last_day_data.loc[ticker]
            todays_close, todays_st, todays_sma = row['CLOSE'], row['ST_15_3'], row['SMA_200']
            
            if pd.notna(todays_close) and ((pd.notna(todays_st) and todays_close < todays_st) or (pd.notna(todays_sma) and todays_close < todays_sma)):
                sells_for_tomorrow.append(ticker)
                
                sell_rows_for_export.append({
                    'Ticker': ticker, 'Action': 'SELL',
                    'Entry_Price': round(todays_close, 2), # Directly show sold at close price
                    'Quantity': pos['qty'], 'Stoploss': '-',
                    'Allocation_%': round(((pos['qty'] * todays_close) / latest_equity) * 100, 2) if latest_equity > 0 else 0,
                    'Entry_Date': pos['entry_date']
                })

    for t in sells_for_tomorrow: del positions[t]

    # Check for Buys based on TODAY'S closing indicators
    buy_candidates = last_day_data[last_day_data['Buy_Signal']].sort_values(by='RS', ascending=False)

    for ticker, row in buy_candidates.iterrows():
        estimated_cost = row['CLOSE'] * (1 + BROKERAGE_RATE)
        available_budget = POSITION_SIZE if capital >= POSITION_SIZE else capital

        if len(positions) < MAX_POSITIONS and ticker not in positions:
            if available_budget > estimated_cost:
                positions[ticker] = {
                    'raw_entry_price': 'Pending Next Open', 'net_entry_price': '-', 'qty': 'TBD',
                    'entry_date': 'Pending Next Open', 'index_st': row['Index_ST_DIR'], 'is_new': True 
                }
            else:
                print(f"Skipping {ticker} on Dashboard: Cannot afford 1 share.")

# --- File 1: Allocations Output ---
alloc_list = []

for t, p in positions.items():
    sl_multiplier = 0.85 if p.get('index_st', 'Up') == 'Down' else 0.87
    action = 'BUY' if p.get('is_new', False) else 'HOLD'
    
    if p['raw_entry_price'] == 'Pending Next Open':
        alloc_list.append({
            'Ticker': t, 'Action': action, 'Entry_Price': 'Pending Next Open',
            'Quantity': 'TBD', 'Stoploss': '-', 'Allocation_%': 'TBD', 'Entry_Date': 'Pending Next Open'
        })
    else:
        alloc_list.append({
            'Ticker': t, 'Action': action, 'Entry_Price': round(p['raw_entry_price'], 2), 
            'Quantity': p['qty'], 'Stoploss': round(p['raw_entry_price'] * sl_multiplier, 2), 
            'Allocation_%': round(((p['qty'] * p['raw_entry_price']) / latest_equity) * 100, 2) if latest_equity > 0 else 0, 
            'Entry_Date': p['entry_date']
        })

alloc_list.extend(sell_rows_for_export)

alloc_df = pd.DataFrame(alloc_list)
alloc_df.to_csv(FILE_1_ALLOCATIONS, index=False)
print(f"✅ Success! Generated targets in {FILE_1_ALLOCATIONS}")

# --- File 2: Historical Portfolio Value Output ---
equity_df = pd.DataFrame(equity_curve)

nifty500_close = nifty500_data[['DATE', 'CLOSE']].rename(columns={'CLOSE': 'Nifty500_Value'})
equity_df = pd.merge(equity_df, nifty500_close, on='DATE', how='left')
equity_df['Nifty500_Value'] = equity_df['Nifty500_Value'].ffill()

if not equity_df['Nifty500_Value'].isna().all():
    first_valid_nifty500 = equity_df['Nifty500_Value'].dropna().iloc[0]
    equity_df['Benchmark_Value'] = (equity_df['Nifty500_Value'] / first_valid_nifty500) * INITIAL_CAPITAL
else:
    equity_df['Benchmark_Value'] = INITIAL_CAPITAL

equity_df['Drawdown'] = equity_df['Equity'] / equity_df['Equity'].cummax() - 1
equity_df['Daily_Return'] = equity_df['Equity'].pct_change()

equity_df.drop(columns=['Nifty500_Value'], inplace=True, errors='ignore')
equity_df.to_csv(FILE_2_PORTFOLIO, index=False)
print(f"✅ Success! Saved history to {FILE_2_PORTFOLIO}")

# --- File 3: Trades Dump Export ---
transaction_ledger = []

if trades:
    for t in trades:
        transaction_ledger.append({
            'Ticker': t['Ticker'], 'Action': 'BOUGHT', 'Date': t['Buy Date'], 'Price': t['Buy Price'], 'Quantity': t['Quantity'], 'Reason': 'Entry',
            'RSI': t['RSI'], '3M Return': t['3M Return'], '6M Return': t['6M Return'], '9M Return': t['9M Return'], 'RS': t['RS'], 'ST Value': t['ST Value'],
            'ST Dir': t['ST Dir'], 'Return %': None, 'PnL ₹': None, 'Holding Days': None
        })
        transaction_ledger.append({
            'Ticker': t['Ticker'], 'Action': 'SOLD', 'Date': t['Sell Date'], 'Price': t['Sell Price'], 'Quantity': t['Quantity'], 'Reason': t['Sell Reason'],
            'RSI': t['RSI'], '3M Return': t['3M Return'], '6M Return': t['6M Return'], '9M Return': t['9M Return'], 'RS': t['RS'], 'ST Value': t['ST Value'],
            'ST Dir': t['ST Dir'], 'Return %': t['Return %'], 'PnL ₹': t['PnL ₹'], 'Holding Days': t['Holding Days']
        })

if positions:
    for ticker, pos in positions.items():
        if pos['raw_entry_price'] != 'Pending Next Open':
            entry_date = pos['entry_date'].date() if hasattr(pos['entry_date'], 'date') else pos['entry_date']
            transaction_ledger.append({
                'Ticker': ticker, 'Action': 'BOUGHT', 'Date': entry_date, 'Price': round(pos['net_entry_price'], 2), 'Quantity': pos['qty'], 'Reason': 'Active Open Position',
                'RSI': pos.get('rsi', None), '3M Return': pos.get('ret_3m', None), '6M Return': pos.get('ret_6m', None), '9M Return': pos.get('ret_9m', None), 
                'RS': pos.get('rs', None), 'ST Value': pos.get('st_val', None), 'ST Dir': pos.get('st_dir', None), 'Return %': None, 'PnL ₹': None, 'Holding Days': None
            })

if transaction_ledger:
    trades_export_df = pd.DataFrame(transaction_ledger)
    trades_export_df['Date'] = pd.to_datetime(trades_export_df['Date'])
    trades_export_df.sort_values(by=['Date', 'Action', 'Ticker'], ascending=[False, True, True], inplace=True)
    trades_export_df['Date'] = trades_export_df['Date'].dt.strftime('%Y-%m-%d')
    trades_export_df.insert(0, 'Sort_Order', range(1, 1 + len(trades_export_df)))
    trades_export_df.to_csv(FILE_3_TRADES, index=False)
    print(f"✅ Success! Exported {len(trades_export_df)} individual transactions to: {FILE_3_TRADES}")
