import pandas as pd
import urllib.request
import json
import time
import pyotp
import os
import requests
import datetime
from SmartApi import SmartConnect

# ==========================================
# 1. CREDENTIALS FROM GITHUB SECRETS
# ==========================================
API_KEY = os.environ.get("ANGEL_API_KEY")
CLIENT_CODE = os.environ.get("ANGEL_CLIENT_CODE")
PIN = os.environ.get("ANGEL_PIN")
TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET")
POWER_BI_URL = os.environ.get("POWER_BI_PUSH_URL") # Ensure this secret is set!

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
            # Format expected by Power BI API
            live_data.append({
                "Symbol": symbol_str,
                # Force CMP to be a float (decimal) so Power BI accepts it
                "CMP": float(ltp_response['data']['ltp']),
                "Last_Updated": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            })
        else:
            print(f"Failed for {symbol_str}: {ltp_response.get('message', 'Unknown error')}")
            
    except Exception as e:
        print(f"Code error on {symbol_str}: {e}")
    
    time.sleep(0.4) # Respect rate limits

# ==========================================
# 4. PUSH DIRECTLY TO POWER BI
# ==========================================
if live_data:
    print(f"Pushing {len(live_data)} records to Power BI...")
    headers = {"Content-Type": "application/json"}
    
    response = requests.post(POWER_BI_URL, json=live_data, headers=headers)
    
    if response.status_code == 200:
        print("Successfully pushed live data to Power BI!")
    else:
        print(f"Failed to push to Power BI. Status Code: {response.status_code}")
        print(response.text)
else:
    print("No data fetched. Nothing to push.")
