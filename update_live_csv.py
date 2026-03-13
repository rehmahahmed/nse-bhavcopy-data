import pandas as pd
import urllib.request
import json
import time
import pyotp
import os
import datetime
from SmartApi import SmartConnect

# ==========================================
# 1. CREDENTIALS
# ==========================================
API_KEY = os.environ.get("ANGEL_API_KEY")
CLIENT_CODE = os.environ.get("ANGEL_CLIENT_CODE")
PIN = os.environ.get("ANGEL_PIN")
TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET")

# ==========================================
# 2. LOGIN & FETCH TOKENS
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
# 3. LOAD SYMBOLS & FETCH LTP
# ==========================================
df_nifty500 = pd.read_csv('nifty750list.csv')
nifty500_symbols = df_nifty500['Symbol'].tolist()

live_data = []
print(f"Fetching live Current Market Prices (CMP) for {len(nifty500_symbols)} stocks...")

# Define timezone offset for accurate timestamps
ist_offset = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

for i, symbol in enumerate(nifty500_symbols):
    symbol_str = str(symbol).strip()
    if symbol_str not in token_map: continue
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            ltp_response = smartApi.ltpData("NSE", f"{symbol_str}-EQ", token_map[symbol_str])
            
            # 1. SUCCESS
            if ltp_response and ltp_response.get('status') and ltp_response.get('data') and 'ltp' in ltp_response['data']:
                cmp = float(ltp_response['data']['ltp'])
                prev_close = float(ltp_response['data']['close'])
                
                # Calculate 1 Day Return (%)
                if prev_close > 0:
                    one_day_return = round(((cmp - prev_close) / prev_close) * 100, 2)
                else:
                    one_day_return = 0.0
                
                live_data.append({
                    "Symbol": symbol_str,
                    "CMP": cmp,
                    "1_Day_Return_%": one_day_return,
                    "Last_Updated": datetime.datetime.now(ist_offset).strftime("%Y-%m-%d %H:%M:%S")
                })
                break 
                
            # 2. RATE LIMITED (Wait and retry)
            elif ltp_response and ltp_response.get('errorcode') == 'AB1004':
                time.sleep(2)
                
            # 3. PERMANENT ERROR (Don't waste time retrying)
            else:
                break
                
        except Exception as e:
            time.sleep(1)
    
    # Optimized base sleep (0.4s keeps us safely under Angel's 3 requests/sec limit)
    time.sleep(0.4) 

    if (i + 1) % 50 == 0:
        print(f"Processed {i + 1} / {len(nifty500_symbols)} stocks...")

# ==========================================
# 4. SAVE DIRECTLY TO CSV
# ==========================================
if live_data:
    df_live = pd.DataFrame(live_data)
    df_live.to_csv('live_cmp.csv', index=False)
    print(f"Successfully updated live_cmp.csv with {len(df_live)} stocks.")
else:
    print("No data fetched. CSV was not updated.")
