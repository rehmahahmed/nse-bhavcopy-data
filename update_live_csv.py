import pandas as pd
import urllib.request
import json
import time
import pyotp
import os
import datetime
from SmartApi import SmartConnect

# ==========================================
# 1. CREDENTIALS FROM GITHUB SECRETS
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
df_nifty500 = pd.read_csv('ind_nifty500list.csv')
nifty500_symbols = df_nifty500['Symbol'].tolist()

live_data = []
print("Fetching live Current Market Prices (CMP)...")

for symbol in nifty500_symbols:
    symbol_str = str(symbol).strip()
    if symbol_str not in token_map: continue
    
    try:
        ltp_response = smartApi.getLTPData("NSE", f"{symbol_str}-EQ", token_map[symbol_str])
        
        if ltp_response['status'] and ltp_response['data']:
            live_data.append({
                "Symbol": symbol_str,
                "CMP": float(ltp_response['data']['ltp']),
                "Last_Updated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        else:
            print(f"Failed for {symbol_str}: {ltp_response.get('message', 'Unknown error')}")
            
    except Exception as e:
        print(f"Code error on {symbol_str}: {e}")
    
    time.sleep(0.4) # Respect rate limits

# ==========================================
# 4. SAVE DIRECTLY TO CSV
# ==========================================
if live_data:
    df_live = pd.DataFrame(live_data)
    df_live.to_csv('live_cmp.csv', index=False)
    print(f"Successfully updated live_cmp.csv with {len(df_live)} stocks.")
else:
    print("No data fetched. CSV was not updated.")
