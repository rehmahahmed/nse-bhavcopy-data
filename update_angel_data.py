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

# The background database needed to calculate rolling metrics (Sharpe, 6M returns, etc.)
HISTORY_FILENAME = "historical_db.csv" 
# The final 500-row dashboard output file you requested
CSV_FILENAME = "daily_rs_data.csv" 

INTERVAL = "ONE_DAY"

# ==========================================
# 2. SMART DATE CALCULATION (SELF-HEALING)
# ==========================================
end_date = datetime.datetime.now()
TO_DATE = end_date.strftime("%Y-%m-%d 15:30")

# We now load from the HISTORY file, not the 500-row dashboard file
if os.path.exists(HISTORY_FILENAME) and os.path.getsize(HISTORY_FILENAME) > 0:
    print(f"Loading existing historical database: {HISTORY_FILENAME}")
    df_existing = pd.read_csv(HISTORY_FILENAME)
    df_existing['Date'] = pd.to_datetime(df_existing['Date'])
    
    last_date = df_existing['Date'].max()
    
    # OVERLAPPING WINDOW: Look back 10 days to heal gaps
    start_date = end_date - datetime.timedelta(days=10)
    print(f"Fetching 10-day overlap from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
else:
    print("No existing historical database found. Fetching full 5-year history...")
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
df_nifty500 = pd.read_csv('nifty750list.csv')
nifty500_symbols = df_nifty500['Symbol'].tolist()

new_data_rows = []

print(f"Fetching data for {len(nifty500_symbols)} stocks...")

for i, symbol in enumerate(nifty500_symbols):
    symbol = str(symbol).strip()
    if symbol not in token_map: continue

    historicParam = {
        "exchange": "NSE", "symboltoken": token_map[symbol],
        "interval": INTERVAL, "fromdate": FROM_DATE, "todate": TO_DATE
    }

    # --- NEW: RETRY LOGIC FOR ERRORS AND NULL VALUES ---
    max_retries = 3
    for attempt in range(max_retries):
        try:
            hist_data = smartApi.getCandleData(historicParam)
            
            # 1. IF SUCCESS: Check if we got a valid status AND actual data inside
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
                break # Success! Break out of the retry loop and move to the next stock
            
            # 2. IF RATE LIMITED: Angel One's specific error code for Too Many Requests
            elif hist_data and hist_data.get('errorcode') == 'AB1004':
                print(f"Rate limited on {symbol}. Cooling down for 3s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(3)
                
            # 3. IF NULL/EMPTY: The API responded, but the data array was empty or null
            else:
                print(f"Null or empty data for {symbol}. Retrying in 2s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(2)
                
        except Exception as e:
            # 4. IF NETWORK ERROR: Disconnects, timeouts, etc.
            print(f"Network error on {symbol}: {e}. Retrying in 2s... (Attempt {attempt+1}/{max_retries})")
            time.sleep(2)
            
    # Base rate limit delay to prevent hitting the wall in the first place
    time.sleep(0.6) 

    # Print progress to the console so you know it hasn't frozen
    if (i + 1) % 50 == 0:
        print(f"Processed {i + 1} / {len(nifty500_symbols)} stocks...")

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
DAYS_1D, DAYS_1W, DAYS_1M = 1, 5, 21
DAYS_3M, DAYS_6M, DAYS_9M, DAYS_12M = 63, 126, 189, 252

df_combined['ret_3m'] = df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_3M)
df_combined['ret_6m'] = df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_6M)
df_combined['ret_9m'] = df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_9M)
df_combined['ret_12m'] = df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_12M)

# Use .fillna(0) so missing long-term data on new stocks doesn't break the entire calculation
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

df_combined['1D Return %'] = (df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_1D) * 100).round(2)
df_combined['1W Return %'] = (df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_1W) * 100).round(2)
df_combined['1M Return %'] = (df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_1M) * 100).round(2)
df_combined['3M Return %'] = (df_combined['ret_3m'] * 100).round(2)
df_combined['6M Return %'] = (df_combined['ret_6m'] * 100).round(2)

df_combined['daily_return_dec'] = df_combined.groupby('Symbol')['Close'].pct_change(1)
rolling_mean = df_combined.groupby('Symbol')['daily_return_dec'].transform(lambda x: x.rolling(window=252, min_periods=126).mean())
rolling_std = df_combined.groupby('Symbol')['daily_return_dec'].transform(lambda x: x.rolling(window=252, min_periods=126).std())

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

# [KEEP YOUR TRY/EXCEPT BLOCK HERE THAT FETCHES THE LIVE BOND YIELD]
daily_rf = live_risk_free_rate / 252

# --- NEW: CALCULATE WEIGHTED SHARPE ---
windows = {'3M': 63, '6M': 126, '9M': 189, '12M': 252}
for suffix, window in windows.items():
    rolling_mean = df_combined.groupby('Symbol')['daily_return_dec'].transform(lambda x: x.rolling(window).mean())
    rolling_std = df_combined.groupby('Symbol')['daily_return_dec'].transform(lambda x: x.rolling(window).std())
    rolling_std = rolling_std.replace(0, np.nan) # Prevent divide-by-zero errors
    df_combined[f'Sharpe_{suffix}'] = ((rolling_mean - daily_rf) / rolling_std) * np.sqrt(252)

# Apply your custom formula (using fillna(0) so recent IPOs don't break the math)
df_combined['Weighted Sharpe'] = (
    0.40 * df_combined['Sharpe_3M'].fillna(0) + 
    0.20 * df_combined['Sharpe_6M'].fillna(0) + 
    0.20 * df_combined['Sharpe_9M'].fillna(0) + 
    0.20 * df_combined['Sharpe_12M'].fillna(0)
).round(2)

# Save the historical database so tomorrow's script has data to work with
df_combined.to_csv(HISTORY_FILENAME, index=False)

# ==========================================
# 6. EXTRACT 500 ROWS FOR DASHBOARD 
# ==========================================
print("Extracting the latest 500 rows for the dashboard...")

# Get ONLY the very last row for each symbol (Yesterday's Close)
df_latest = df_combined.groupby('Symbol').tail(1).copy()

# Add the Industry column
if 'Industry' in df_latest.columns:
    df_latest = df_latest.drop(columns=['Industry'])
df_final = pd.merge(df_latest, df_nifty500[['Symbol', 'Industry']], on='Symbol', how='left')

# --- NEW: ADD LAST UPDATED TIMESTAMP ---
# Grab the current system time and format it cleanly
update_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df_final['Last_Updated'] = update_time_str

# --- NEW: ADD ROLLING 6-MONTH CHART LINK ---
# We use Google Finance because it accepts the '?window=6M' parameter to force the timeframe
df_final['Chart_Link'] = "https://www.google.com/finance/quote/" + df_final['Symbol'] + ":NSE?window=6M"

# Keep ONLY the columns you requested, plus the new link
final_columns = [
    'Symbol', 'Industry', '1D Return %', '1W Return %', '1M Return %', 
    '3M Return %', '6M Return %', 'Weighted Sharpe', 'weighted_avg', 'RS', 'Last_Updated', 'Chart_Link'
]
df_final = df_final[final_columns]

# Overwrite the daily_rs_data.csv with exactly 500 rows
df_final.to_csv(CSV_FILENAME, index=False)
print(f"Success! Dashboard file '{CSV_FILENAME}' generated. Last Updated: {update_time_str}")
