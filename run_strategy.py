import pandas as pd
import numpy as np
import pandas_ta as ta
import yfinance as yf
import os
import warnings
import urllib.request
import json
import pyotp
import time
from datetime import datetime, timedelta
import pytz
from SmartApi import SmartConnect

warnings.filterwarnings('ignore')

# --- CONFIGURATION ---
FILE_1_ALLOCATIONS = "daily_allocations.csv"
FILE_2_PORTFOLIO = "historical_portfolio_value.csv"
TICKER_FILE = "ind_nifty500list.csv"

MAX_POSITIONS = 6
INITIAL_CAPITAL = 100000.0

ist = pytz.timezone('Asia/Kolkata')
now_ist = datetime.now(ist)

# Fetch 5 years + 250 days to ensure the 200 SMA and 12M Returns have enough runway to calculate
start_date_str = (now_ist - timedelta(days=(5*365) + 250)).strftime('%Y-%m-%d')

print(f"Running Standalone EOD Strategy. Time (IST): {now_ist.strftime('%Y-%m-%d %H:%M:%S')}")

# ==========================================
# 0. ANGEL ONE LOGIN (For 3 PM Live CMP)
# ==========================================
API_KEY = os.environ.get("ANGEL_API_KEY")
CLIENT_CODE = os.environ.get("ANGEL_CLIENT_CODE")
PIN = os.environ.get("ANGEL_PIN")
TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET")

smartApi = None
token_map = {}

if API_KEY and CLIENT_CODE:
    print("Connecting to Angel One API to fetch Live 3 PM CMPs...")
    try:
        smartApi = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_SECRET).now()
        login_data = smartApi.generateSession(CLIENT_CODE, PIN, totp)
        
        if login_data['status']:
            instrument_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
            response = urllib.request.urlopen(instrument_url)
            instrument_list = json.loads(response.read())
            token_map = {inst['symbol'].replace('-EQ', ''): inst['token'] 
                         for inst in instrument_list if inst['exch_seg'] == 'NSE' and inst['symbol'].endswith('-EQ')}
            print("Successfully authenticated with Angel One.")
        else:
            print("Angel Login Failed:", login_data['message'])
    except Exception as e:
        print(f"Error during Angel One authentication: {e}")

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

print(f"Downloading historical data for {len(nifty_tickers)} tickers from {start_date_str}...")
raw_data = yf.download(nifty_tickers, start=start_date_str, progress=False, auto_adjust=False)

df = raw_data.stack(level=1, future_stack=True).reset_index()
df.rename(columns={'Date': 'DATE', 'Ticker': 'TICKER', 'Open': 'OPEN', 'High': 'HIGH', 'Low': 'LOW', 'Close': 'CLOSE'}, inplace=True)
df['DATE'] = pd.to_datetime(df['DATE']).dt.tz_localize(None)
df.dropna(subset=['CLOSE'], inplace=True)

# Fetch Index Data for Regime Filter
print("Downloading Nifty 500 Index data for regime filter...")
index_data = yf.download("^CRSLDX", start=start_date_str, progress=False, auto_adjust=False)
if index_data.empty:
    index_data = yf.download("^NSEI", start=start_date_str, progress=False, auto_adjust=False)

if isinstance(index_data.columns, pd.MultiIndex):
    index_data.columns = index_data.columns.droplevel(1)

index_data = index_data.reset_index()
index_data.rename(columns={'Date': 'DATE', 'High': 'HIGH', 'Low': 'LOW', 'Close': 'CLOSE'}, inplace=True)
index_data['DATE'] = pd.to_datetime(index_data['DATE']).dt.tz_localize(None)

st_idx = ta.supertrend(high=index_data['HIGH'], low=index_data['LOW'], close=index_data['CLOSE'], length=15, multiplier=2.75)
index_data['Index_ST_DIR'] = st_idx.iloc[:, 1].map({1: 'Up', -1: 'Down'}) if st_idx is not None else 'Up'
index_regime = index_data[['DATE', 'Index_ST_DIR']].dropna()

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

df['Weighted Avg'] = (0.3 * df['3M_Return'] + 0.3 * df['6M_Return'] + 0.2 * df['9M_Return'] + 0.2 * df['12M_Return'])
df['RS_Score'] = df.groupby('DATE')['Weighted Avg'].rank(pct=True) * 100
df['RS_Score'] = df['RS_Score'].round(0).clip(lower=1, upper=99)

df_bt = df.dropna(subset=['SMA_200', '12M_Return', 'RS_Score', 'ST_15_3']).copy()

# Restrict backtest starting point to exactly 5 years ago
backtest_start = pd.to_datetime((now_ist - timedelta(days=5*365)).strftime('%Y-%m-%d'))
df_bt = df_bt[df_bt['DATE'] >= backtest_start]

df_bt = pd.merge(df_bt, index_regime, on='DATE', how='left')
df_bt['Index_ST_DIR'] = df_bt['Index_ST_DIR'].ffill().fillna('Up')

# ==========================================
# 3. SIGNAL LOGIC & BACKTEST ENGINE
# ==========================================
is_not_circuit = df_bt['HIGH'] != df_bt['LOW']
is_index_down = df_bt['Index_ST_DIR'] == 'Down'

rsi_threshold = np.where(is_index_down, 55, 55)
return_1d_threshold = np.where(is_index_down, -0.05, -0.05)
rs_score_threshold = np.where(is_index_down, 95, 80)

stock_selection = ((df_bt['RSI_14'] >= rsi_threshold) & (df_bt['1D_Return'] > return_1d_threshold) & 
                   ((df_bt['3M_Return'] > 0.20) | (df_bt['6M_Return'] > 0.30) | (df_bt['1M_Return'] > 0.10)) & 
                   (df_bt['SMA_50'] > df_bt['SMA_200']))

buy_condition = ((df_bt['RS_Score'] > rs_score_threshold) & (df_bt['CLOSE'] > df_bt['SMA_200']) & 
                 (df_bt['CLOSE'] > df_bt['ST_15_3']) & (df_bt['CLOSE'] >= df_bt['EMA_9'] * 0.95) & 
                 (df_bt['CLOSE'] <= df_bt['EMA_9'] * 1.05))

df_bt['Buy_Signal'] = is_not_circuit & stock_selection & buy_condition

shift_cols = ['Buy_Signal', 'CLOSE', 'ST_15_3', 'SMA_200', 'RS_Score', 'RSI_14', '3M_Return', '6M_Return', '9M_Return', 'ST_DIR', 'Index_ST_DIR']
for col in shift_cols:
    df_bt[f'Prev_{col}'] = df_bt.groupby('TICKER')[col].shift(1)

df_bt['Prev_Buy_Signal'] = df_bt['Prev_Buy_Signal'].fillna(False).astype(bool)

print("Running Backtest Engine with Dynamic Position Sizing (Compounding)...")
capital = INITIAL_CAPITAL
positions = {}
equity_curve = []
unique_dates = sorted(df_bt['DATE'].unique())
BROKERAGE_RATE = 0.005

for current_date in unique_dates:
    daily_data = df_bt[df_bt['DATE'] == current_date].set_index('TICKER')
    tickers_to_remove = []
    
    # Process Sells
    for ticker, pos in positions.items():
        if ticker in daily_data.index:
            row = daily_data.loc[ticker]
            today_open, today_low, today_high = row['OPEN'], row['LOW'], row['HIGH']
            prev_close, prev_st, prev_sma, prev_index_st = row['Prev_CLOSE'], row['Prev_ST_15_3'], row['Prev_SMA_200'], row['Prev_Index_ST_DIR']
            
            if today_high == today_low: continue

            triggered_sell = False
            if pd.notna(prev_close) and pd.notna(prev_st) and prev_close < prev_st:
                exit_price = today_open
                triggered_sell = True
            elif pd.notna(prev_close) and pd.notna(prev_sma) and prev_close < prev_sma:
                exit_price = today_open
                triggered_sell = True
            
            if not triggered_sell:
                sl_multiplier = 0.85 if prev_index_st == 'Down' else 0.87
                sl_price = pos['raw_entry_price'] * sl_multiplier
                if today_open <= sl_price:
                    exit_price = today_open
                    triggered_sell = True
                elif today_low <= sl_price:
                    exit_price = sl_price
                    triggered_sell = True

            if triggered_sell:
                net_exit_price = exit_price * (1 - BROKERAGE_RATE)
                capital += (net_exit_price * pos['qty'])
                tickers_to_remove.append(ticker)

    for t in tickers_to_remove: del positions[t]

    # Process Buys
    buy_signals = daily_data[daily_data['Prev_Buy_Signal']].sort_values(by='Prev_RS_Score', ascending=False)
    
    # NEW: Calculate current total equity to determine dynamic position size
    current_portfolio_value = capital
    for t, p in positions.items():
        if t in daily_data.index:
            current_portfolio_value += p['qty'] * daily_data.loc[t, 'CLOSE']
        else:
            current_portfolio_value += p['qty'] * p['raw_entry_price']
            
    dynamic_position_size = current_portfolio_value / MAX_POSITIONS

    for ticker, row in buy_signals.iterrows():
        if row['HIGH'] == row['LOW']: continue
        if len(positions) < MAX_POSITIONS and ticker not in positions:
            
            # Use dynamic position sizing so the strategy reinvests profits
            invest_amount = dynamic_position_size if capital >= dynamic_position_size else capital
            execution_price = row['OPEN']
            net_buy_price = execution_price * (1 + BROKERAGE_RATE)

            if invest_amount > net_buy_price:
                qty = int(invest_amount // net_buy_price)
                cost = qty * net_buy_price
                positions[ticker] = {
                    'raw_entry_price': execution_price, 'qty': qty, 
                    'entry_date': current_date, 'index_st': row['Prev_Index_ST_DIR'],
                    'is_new': False 
                }
                capital -= cost

    # Record Daily Equity
    daily_portfolio_value = capital
    for ticker, pos in positions.items():
        if ticker in daily_data.index:
            daily_portfolio_value += pos['qty'] * daily_data.loc[ticker, 'CLOSE']
        else:
            daily_portfolio_value += pos['qty'] * pos['raw_entry_price']
    equity_curve.append({'Date': current_date, 'Equity': daily_portfolio_value})

# ==========================================
# 4. PREPARE TODAY'S 3:00 PM TARGETS & EXPORT
# ==========================================
latest_date = unique_dates[-1]
last_day_data = df_bt[df_bt['DATE'] == latest_date].set_index('TICKER')
latest_equity = equity_curve[-1]['Equity'] if equity_curve else INITIAL_CAPITAL

# We need to simulate the available cash for today's 3 PM buys
simulated_capital = capital
sells_for_today = []

for ticker, pos in positions.items():
    if ticker in last_day_data.index:
        row = last_day_data.loc[ticker]
        todays_close, todays_st, todays_sma = row['CLOSE'], row['ST_15_3'], row['SMA_200']
        if pd.notna(todays_close) and ((pd.notna(todays_st) and todays_close < todays_st) or (pd.notna(todays_sma) and todays_close < todays_sma)):
            sells_for_today.append(ticker)
            # Add cash back to simulated pool for new buys
            simulated_capital += pos['qty'] * todays_close * (1 - BROKERAGE_RATE)

for t in sells_for_today: del positions[t]

buy_candidates = last_day_data[last_day_data['Buy_Signal']].sort_values(by='RS_Score', ascending=False)
dynamic_position_size = latest_equity / MAX_POSITIONS

for ticker, row in buy_candidates.iterrows():
    if len(positions) < MAX_POSITIONS and ticker not in positions:
        
        # Fetch Live CMP from Angel at 3 PM
        clean_ticker = ticker.replace('.NS', '')
        cmp = None
        
        if smartApi and clean_ticker in token_map:
            try:
                ltp_response = smartApi.ltpData("NSE", f"{clean_ticker}-EQ", token_map[clean_ticker])
                if ltp_response and ltp_response.get('status') and ltp_response.get('data'):
                    cmp = float(ltp_response['data']['ltp'])
            except Exception as e:
                pass
        
        # Fallback to the latest available close if Angel is unavailable
        if cmp is None:
            cmp = row['CLOSE']
            
        invest_amount = dynamic_position_size if simulated_capital >= dynamic_position_size else simulated_capital
        net_buy_price = cmp * (1 + BROKERAGE_RATE)
        qty = int(invest_amount // net_buy_price) if net_buy_price > 0 else 0

        if qty > 0:
            positions[ticker] = {
                'raw_entry_price': cmp,
                'qty': qty,
                'entry_date': now_ist.strftime('%Y-%m-%d'),
                'index_st': row['Index_ST_DIR'],
                'is_new': True 
            }
            simulated_capital -= (qty * net_buy_price)

# --- File 1: Allocations Output ---
alloc_list = []

for t, p in positions.items():
    sl_multiplier = 0.85 if p.get('index_st', 'Up') == 'Down' else 0.87
    action = 'BUY' if p.get('is_new', False) else 'HOLD'
    
    alloc_list.append({
        'Ticker': t, 
        'Action': action, 
        'Entry_Price': round(p['raw_entry_price'], 2), 
        'Quantity': p['qty'], 
        'Stoploss': round(p['raw_entry_price'] * sl_multiplier, 2), 
        'Allocation_%': round(((p['qty'] * p['raw_entry_price']) / latest_equity) * 100, 2) if latest_equity > 0 else 0, 
        'Entry_Date': p['entry_date']
    })

alloc_df = pd.DataFrame(alloc_list)
alloc_df.to_csv(FILE_1_ALLOCATIONS, index=False)
print(f"✅ Success! Generated {len(alloc_df)} targets in {FILE_1_ALLOCATIONS}")

# --- File 2: Historical Portfolio Value Output ---
equity_df = pd.DataFrame(equity_curve)
equity_df['Drawdown'] = equity_df['Equity'] / equity_df['Equity'].cummax() - 1
equity_df['Daily_Return'] = equity_df['Equity'].pct_change()
equity_df.to_csv(FILE_2_PORTFOLIO, index=False)
print(f"✅ Success! Saved {len(equity_df)} days of history to {FILE_2_PORTFOLIO}")
