import pandas as pd
import datetime
import time
import urllib.request
import json
import pyotp
import os
from SmartApi import SmartConnect
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. CREDENTIALS & CONFIGURATION
# ==========================================
API_KEY = os.environ.get("ANGEL_API_KEY")
CLIENT_CODE = os.environ.get("ANGEL_CLIENT_CODE")
PIN = os.environ.get("ANGEL_PIN")
TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET")

INPUT_FILE = "nifty750list.csv"
OUTPUT_FILE = "market_breadth_history_5yr.csv"
INTERVAL = "ONE_DAY"

# --- 5-Year Time Calculation with DMA Padding ---
end_date = datetime.datetime.now()
five_years_ago = end_date - datetime.timedelta(days=5 * 365)

# We need 200 trading days (~300 calendar days) BEFORE our 5-year start date 
# so the 200 SMA can calculate properly on day one.
start_date = five_years_ago - datetime.timedelta(days=300)

TO_DATE = end_date.strftime("%Y-%m-%d 15:30")
FROM_DATE = start_date.strftime("%Y-%m-%d 09:15")

# ==========================================
# 2. LOGIN & FETCH TOKENS
# ==========================================
print("Logging into Angel One...")
smartApi = SmartConnect(api_key=API_KEY)
totp = pyotp.TOTP(TOTP_SECRET).now()
login_data = smartApi.generateSession(CLIENT_CODE, PIN, totp)

if not login_data['status']:
    print("Login Failed:", login_data['message'])
    exit()

print("Fetching instrument tokens...")
instrument_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
response = urllib.request.urlopen(instrument_url)
instrument_list = json.loads(response.read())

token_map = {inst['symbol'].replace('-EQ', ''): inst['token'] 
             for inst in instrument_list if inst['exch_seg'] == 'NSE' and inst['symbol'].endswith('-EQ')}

# ==========================================
# 3. LOAD SYMBOLS & FETCH DATA
# ==========================================
try:
    df_tickers = pd.read_csv(INPUT_FILE)
    symbols = df_tickers['Symbol'].tolist()
except Exception as e:
    print(f"Error reading {INPUT_FILE}: {e}")
    exit()

print(f"Fetching ~6 years of history (to pad the 200 SMA) for {len(symbols)} stocks...")
print("This will take roughly 30 to 45 minutes to avoid API bans. Please wait...")
raw_data_rows = []

for i, symbol in enumerate(symbols):
    symbol = str(symbol).strip()
    if symbol not in token_map: continue

    historicParam = {
        "exchange": "NSE", "symboltoken": token_map[symbol],
        "interval": INTERVAL, "fromdate": FROM_DATE, "todate": TO_DATE
    }

    # RETRY LOGIC FOR RATE LIMITS AND NULLS
    max_retries = 3
    for attempt in range(max_retries):
        try:
            hist_data = smartApi.getCandleData(historicParam)
            
            if hist_data and hist_data.get('status') and hist_data.get('data'):
                for row in hist_data['data']:
                    raw_data_rows.append({
                        'Date': row[0][:10],
                        'Symbol': symbol,
                        'Close': row[4]
                    })
                break 
            
            elif hist_data and hist_data.get('errorcode') == 'AB1004':
                print(f"Rate limited on {symbol}. Cooling down for 3s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(3) 
            
            else:
                break 

        except Exception as e:
            print(f"Network error on {symbol}: {e}. Retrying...")
            time.sleep(2)
            
    time.sleep(0.6) 
    
    if (i + 1) % 50 == 0:
        print(f"Processed {i + 1} / {len(symbols)} stocks...")

# ==========================================
# 4. CALCULATE HISTORICAL METRICS
# ==========================================
if not raw_data_rows:
    print("No data fetched from Angel API. Exiting.")
    exit()

print("\nPivoting data and calculating the 200 SMA...")
df_all = pd.DataFrame(raw_data_rows)

# Pivot so Dates are rows and Symbols are columns
df_close = df_all.pivot(index='Date', columns='Symbol', values='Close')
df_close.index = pd.to_datetime(df_close.index)
df_close = df_close.sort_index()

# Calculate the 200 Simple Moving Average for the entire matrix
sma_200 = df_close.rolling(window=200).mean()

# ==========================================
# 5. AGGREGATE COUNTS & SAVE (EXACTLY 5 YEARS)
# ==========================================
print("Aggregating breadth metric...")

# Create an empty dataframe with our Dates as the index
df_breadth = pd.DataFrame(index=df_close.index)

# Vectorized counting: Checks how many stocks are > their 200 SMA each day
df_breadth['Stocks_Above_200_SMA'] = (df_close > sma_200).sum(axis=1)

# Slice the dataframe to exactly the 5-year mark
cutoff_date_str = five_years_ago.strftime('%Y-%m-%d')
df_breadth = df_breadth.loc[cutoff_date_str:]

# Convert the Date index back into a standard column for the CSV
df_breadth = df_breadth.reset_index()
df_breadth['Date'] = df_breadth['Date'].dt.strftime('%Y-%m-%d')

df_breadth.to_csv(OUTPUT_FILE, index=False)

print(f"\n[SUCCESS] Generated exact 5-year breadth history (Starting {cutoff_date_str}).")
print(f"Saved to {OUTPUT_FILE}")
print(f"Total trading days recorded: {len(df_breadth)}")
