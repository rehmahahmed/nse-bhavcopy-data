import pandas as pd
import datetime
import time
import urllib.request
import json
import pyotp
import numpy as np
import os
from SmartApi import SmartConnect
import yfinance as yf

# ==========================================
# 1. CREDENTIALS & CONFIGURATION
# ==========================================
API_KEY = os.environ.get("ANGEL_API_KEY")
CLIENT_CODE = os.environ.get("ANGEL_CLIENT_CODE")
PIN = os.environ.get("ANGEL_PIN")
TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET")

# File paths
INPUT_LIST_FILENAME = "nifty750list.csv"
HISTORY_FILENAME = "historical_db.csv" 
CSV_FILENAME = "daily_rs_data.csv" 

INTERVAL = "ONE_DAY"

# --- NEW: Ensure destination directories exist if you ever use subfolders ---
def ensure_dir_exists(filepath):
    directory = os.path.dirname(filepath)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

ensure_dir_exists(HISTORY_FILENAME)
ensure_dir_exists(CSV_FILENAME)

# ==========================================
# 2. SMART DATE CALCULATION (SELF-HEALING)
# ==========================================
end_date = datetime.datetime.now()
TO_DATE = end_date.strftime("%Y-%m-%d 15:30")

if os.path.exists(HISTORY_FILENAME) and os.path.getsize(HISTORY_FILENAME) > 0:
    print(f"Loading existing historical database: {HISTORY_FILENAME}")
    df_existing = pd.read_csv(HISTORY_FILENAME)
    df_existing['Date'] = pd.to_datetime(df_existing['Date'])
    
    last_date = df_existing['Date'].max()
    
    # OVERLAPPING WINDOW: Look back 10 days to heal gaps
    start_date = end_date - datetime.timedelta(days=10)
    print(f"Fetching 10-day overlap from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
else:
    print(f"No existing historical database found at '{HISTORY_FILENAME}'. Fetching full 5-year history...")
    df_existing = pd.DataFrame()
    start_date = end_date - datetime.timedelta(days=5*365)

FROM_DATE = start_date.strftime("%Y-%m-%d 09:15")

if start_date.date() > end_date.date():
    print("Data is already fully up to date! Exiting.")
    exit()

# ==========================================
# 3. LOGIN & FETCH TOKENS
# ==========================================
smartApi = SmartConnect(api_key=API_KEY)
totp = pyotp.TOTP(TOTP_SECRET).now()
login_data = smartApi.generateSession(CLIENT_CODE, PIN, totp)

if not login_data['status']:
    print("Login Failed:", login_data['message'])
    exit()

instrument_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
response = urllib.request.urlopen(instrument_url)
instrument_list = json.loads(response.read())

token_map = {inst['symbol'].replace('-EQ', ''): inst['token'] 
             for inst in instrument_list if inst['exch_seg'] == 'NSE' and inst['symbol'].endswith('-EQ')}

# ==========================================
# 4. LOAD SYMBOLS & FETCH MISSING DATA
# ==========================================
# --- NEW: Crash prevention if the input list is missing ---
if not os.path.exists(INPUT_LIST_FILENAME):
    print(f"CRITICAL ERROR: Could not find '{INPUT_LIST_FILENAME}'.")
    print("Please make sure you have downloaded the Nifty 750 CSV from NSE and placed it in the same folder as this script.")
    exit()

df_nifty750 = pd.read_csv(INPUT_LIST_FILENAME)
nifty750_symbols = df_nifty750['Symbol'].tolist()

new_data_rows = []

print(f"Fetching data for {len(nifty750_symbols)} stocks...")

for i, symbol in enumerate(nifty750_symbols):
    symbol = str(symbol).strip()
    if symbol not in token_map: continue

    historicParam = {
        "exchange": "NSE", "symboltoken": token_map[symbol],
        "interval": INTERVAL, "fromdate": FROM_DATE, "todate": TO_DATE
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            hist_data = smartApi.getCandleData(historicParam)
            
            if hist_data and hist_data.get('status') and hist_data.get('data'):
                for row in hist_data['data']:
                    date_str = row[0][:10]
                    new_data_rows.append({
                        'Date': date_str,
                        'Symbol': symbol,
                        'Open': row[1],
                        'High': row[2],
                        'Low': row[3],
                        'Close': row[4],
                        'Volume': row[5]
                    })
                break 
            
            elif hist_data and hist_data.get('errorcode') == 'AB1004':
                print(f"Rate limited on {symbol}. Cooling down for 3s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(3)
                
            else:
                print(f"Null or empty data for {symbol}. Retrying in 2s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(2)
                
        except Exception as e:
            print(f"Network error on {symbol}: {e}. Retrying in 2s... (Attempt {attempt+1}/{max_retries})")
            time.sleep(2)
            
    time.sleep(0.6) 

    if (i + 1) % 50 == 0:
        print(f"Processed {i + 1} / {len(nifty750_symbols)} stocks...")

# ==========================================
# 5. COMBINE & CALCULATE ALL METRICS
# ==========================================
if new_data_rows:
    df_new = pd.DataFrame(new_data_rows)
    df_new['Date'] = pd.to_datetime(df_new['Date'])
    
    if not df_existing.empty:
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['Date', 'Symbol'], keep='last')
    else:
        df_combined = df_new
else:
    print("No new data was returned from the API.")
    df_combined = df_existing

if df_combined.empty:
    print("No data available to process.")
    exit()

df_combined = df_combined.dropna(subset=['Close'])
print("Calculating RS, Return percentages, and Sharpe...")
df_combined = df_combined.sort_values(by=['Symbol', 'Date']).reset_index(drop=True)

# Define trading day periods
DAYS_1W, DAYS_1M = 5, 21
DAYS_3M, DAYS_6M, DAYS_9M, DAYS_12M = 63, 126, 189, 252

df_combined['ret_3m'] = df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_3M)
df_combined['ret_6m'] = df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_6M)
df_combined['ret_9m'] = df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_9M)
df_combined['ret_12m'] = df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_12M)

df_combined['weighted_avg'] = (0.40 * df_combined['ret_3m'].fillna(0)) + \
                              (0.20 * df_combined['ret_6m'].fillna(0)) + \
                              (0.20 * df_combined['ret_9m'].fillna(0)) + \
                              (0.20 * df_combined['ret_12m'].fillna(0))

def calculate_daily_rank(x):
    valid_counts = x.notna().sum()
    if valid_counts > 1:
        return (x.rank(method='min') - 1) / (valid_counts - 1) * 100
    return np.nan

df_combined['RS'] = df_combined.groupby('Date')['weighted_avg'].transform(calculate_daily_rank).round(0)
df_combined['RS'] = np.where(df_combined['RS'] == 0, 1, df_combined['RS'])
df_combined['RS'] = np.where(df_combined['RS'] == 100, 99, df_combined['RS'])

df_combined['1W Return %'] = (df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_1W) * 100).round(2)
df_combined['1M Return %'] = (df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_1M) * 100).round(2)
df_combined['3M Return %'] = (df_combined['ret_3m'] * 100).round(2)
df_combined['6M Return %'] = (df_combined['ret_6m'] * 100).round(2)

df_combined['daily_return_dec'] = df_combined.groupby('Symbol')['Close'].pct_change(1)

try:
    print("Fetching live India 10-Year Bond yield for Sharpe calculation...")
    bond_ticker = yf.Ticker("^IN10YT")
    bond_data = bond_ticker.history(period="1d")
    if not bond_data.empty:
        live_risk_free_rate = bond_data['Close'].iloc[-1] / 100.0
    else:
        raise ValueError("Empty DataFrame.")
except Exception:
    live_risk_free_rate = 0.07

daily_rf = live_risk_free_rate / 252

windows = {'3M': 63, '6M': 126, '9M': 189, '12M': 252}
for suffix, window in windows.items():
    rolling_mean = df_combined.groupby('Symbol')['daily_return_dec'].transform(lambda x: x.rolling(window).mean())
    rolling_std = df_combined.groupby('Symbol')['daily_return_dec'].transform(lambda x: x.rolling(window).std())
    rolling_std = rolling_std.replace(0, np.nan) 
    df_combined[f'Sharpe_{suffix}'] = ((rolling_mean - daily_rf) / rolling_std) * np.sqrt(252)

df_combined['Weighted Sharpe'] = (
    0.40 * df_combined['Sharpe_3M'].fillna(0) + 
    0.20 * df_combined['Sharpe_6M'].fillna(0) + 
    0.20 * df_combined['Sharpe_9M'].fillna(0) + 
    0.20 * df_combined['Sharpe_12M'].fillna(0)
).round(2)

# Save history file - pandas will automatically create the file if it doesn't exist
df_combined.to_csv(HISTORY_FILENAME, index=False)

# ==========================================
# 6. EXTRACT DASHBOARD DATA
# ==========================================
print("Extracting the latest rows for the dashboard...")

df_latest = df_combined.groupby('Symbol').tail(1).copy()

if 'Industry' in df_latest.columns:
    df_latest = df_latest.drop(columns=['Industry'])
df_final = pd.merge(df_latest, df_nifty750[['Symbol', 'Industry']], on='Symbol', how='left')

update_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df_final['Last_Updated'] = update_time_str
df_final['Chart_Link'] = "https://www.google.com/finance/quote/" + df_final['Symbol'] + ":NSE?window=6M"

# --- CHANGE: 'Close' column added to the dashboard output ---
final_columns = [
    'Symbol', 'Industry', 'Close', '1W Return %', '1M Return %', 
    '3M Return %', '6M Return %', 'Weighted Sharpe', 'weighted_avg', 'RS', 'Last_Updated', 'Chart_Link'
]
df_final = df_final[final_columns]

# Save final dashboard file - pandas will automatically create the file if it doesn't exist
df_final.to_csv(CSV_FILENAME, index=False)
print(f"Success! Dashboard file '{CSV_FILENAME}' generated. Last Updated: {update_time_str}")
