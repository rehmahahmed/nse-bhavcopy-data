import pandas as pd
import datetime
import time
import urllib.request
import json
import pyotp
import numpy as np
import os
from SmartApi import SmartConnect

# ==========================================
# 1. CREDENTIALS FROM GITHUB SECRETS
# ==========================================
API_KEY = os.environ.get("ANGEL_API_KEY")
CLIENT_CODE = os.environ.get("ANGEL_CLIENT_CODE")
PIN = os.environ.get("ANGEL_PIN")
TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET")

CSV_FILENAME = "daily_rs_data.csv"
INTERVAL = "ONE_DAY"

# ==========================================
# 2. SMART DATE CALCULATION
# ==========================================
end_date = datetime.datetime.now()
TO_DATE = end_date.strftime("%Y-%m-%d 15:30")

if os.path.exists(CSV_FILENAME) and os.path.getsize(CSV_FILENAME) > 0:
    print(f"Loading existing database: {CSV_FILENAME}")
    df_existing = pd.read_csv(CSV_FILENAME)
    df_existing['Date'] = pd.to_datetime(df_existing['Date'])
    
    last_date = df_existing['Date'].max()
    start_date = last_date + datetime.timedelta(days=1)
    
    print(f"Existing data up to {last_date.strftime('%Y-%m-%d')}.")
    print(f"Fetching missing data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
else:
    print("No existing database found. Fetching full 5-year history...")
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
df_nifty500 = pd.read_csv('ind_nifty500list.csv')
nifty500_symbols = df_nifty500['Symbol'].tolist()

new_data_rows = []

for symbol in nifty500_symbols:
    symbol = str(symbol).strip()
    if symbol not in token_map: continue

    historicParam = {
        "exchange": "NSE", "symboltoken": token_map[symbol],
        "interval": INTERVAL, "fromdate": FROM_DATE, "todate": TO_DATE
    }

    try:
        hist_data = smartApi.getCandleData(historicParam)
        if hist_data['status'] and hist_data['data']:
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
    except Exception as e:
        pass
    time.sleep(0.4) 

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

print("Calculating RS, Return percentages, and Sharpe...")

df_combined = df_combined.sort_values(by=['Symbol', 'Date']).reset_index(drop=True)

# Define trading day periods
DAYS_1D, DAYS_1W, DAYS_1M = 1, 5, 21
DAYS_3M, DAYS_6M, DAYS_9M, DAYS_12M = 63, 126, 189, 252

# Base decimal calculations for the RS math
df_combined['ret_3m'] = df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_3M)
df_combined['ret_6m'] = df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_6M)
df_combined['ret_9m'] = df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_9M)
df_combined['ret_12m'] = df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_12M)

df_combined['weighted_avg'] = (0.40 * df_combined['ret_3m']) + \
                              (0.20 * df_combined['ret_6m']) + \
                              (0.20 * df_combined['ret_9m']) + \
                              (0.20 * df_combined['ret_12m'])

def calculate_daily_rank(x):
    valid_counts = x.notna().sum()
    if valid_counts > 1:
        return (x.rank(method='min') - 1) / (valid_counts - 1) * 100
    return np.nan

df_combined['RS'] = df_combined.groupby('Date')['weighted_avg'].transform(calculate_daily_rank).round(0)
df_combined['RS'] = np.where(df_combined['RS'] == 0, 1, df_combined['RS'])
df_combined['RS'] = np.where(df_combined['RS'] == 100, 99, df_combined['RS'])

# Calculate the new Display Return % columns
df_combined['1D Return %'] = (df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_1D) * 100).round(2)
df_combined['1W Return %'] = (df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_1W) * 100).round(2)
df_combined['1M Return %'] = (df_combined.groupby('Symbol')['Close'].pct_change(periods=DAYS_1M) * 100).round(2)
df_combined['3M Return %'] = (df_combined['ret_3m'] * 100).round(2)
df_combined['6M Return %'] = (df_combined['ret_6m'] * 100).round(2)

# --- NEW: CALCULATE ROLLING SHARPE RATIO ---
# 1. Calculate raw daily decimal returns
df_combined['daily_return_dec'] = df_combined.groupby('Symbol')['Close'].pct_change(1)

# 2. Calculate rolling 252-day mean and standard deviation (minimum 126 days/6 months required)
rolling_mean = df_combined.groupby('Symbol')['daily_return_dec'].transform(lambda x: x.rolling(window=252, min_periods=126).mean())
rolling_std = df_combined.groupby('Symbol')['daily_return_dec'].transform(lambda x: x.rolling(window=252, min_periods=126).std())

# 3. Apply Annualized Sharpe Formula (Assume 5% Indian Risk-Free Rate)
risk_free_rate = 0.05
daily_rf = risk_free_rate / 252

df_combined['Sharpe'] = ((rolling_mean - daily_rf) / rolling_std) * np.sqrt(252)
df_combined['Sharpe'] = df_combined['Sharpe'].round(2)

# ==========================================
# 6. FINAL CLEANUP & MERGE
# ==========================================
# Prevent Industry_x/Industry_y conflicts by dropping the old column before merging
if 'Industry' in df_combined.columns:
    df_combined = df_combined.drop(columns=['Industry'])

# Merge with the Nifty 500 list to bring in the fresh Industry column
df_final = pd.merge(df_combined, df_nifty500[['Symbol', 'Industry']], on='Symbol', how='left')

# Reorder columns perfectly for Power BI, including the new returns and Sharpe
final_columns = [
    'Date', 'Symbol', 'Industry', 'Open', 'High', 'Low', 'Close', 'Volume', 
    '1D Return %', '1W Return %', '1M Return %', '3M Return %', '6M Return %', 
    'Sharpe', 'weighted_avg', 'RS'
]
df_final = df_final[final_columns]

# Save the master file back to the root directory
df_final.to_csv(CSV_FILENAME, index=False)
print(f"Success! Master database '{CSV_FILENAME}' has been updated with Return % columns and Sharpe Ratio.")
