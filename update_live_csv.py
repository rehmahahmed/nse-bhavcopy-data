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
print(f"Fetching live Current Market Prices (CMP) for {len(nifty500_symbols)} stocks...")

for i, symbol in enumerate(nifty500_symbols):
    symbol_str = str(symbol).strip()
    if symbol_str not in token_map: continue
    
    # --- NEW: RETRY LOGIC FOR ERRORS AND NULL VALUES ---
    max_retries = 3
    for attempt in range(max_retries):
        try:
            ltp_response = smartApi.ltpData("NSE", f"{symbol_str}-EQ", token_map[symbol_str])
            
            # 1. IF SUCCESS: Check if we got a valid status AND the 'ltp' key actually exists
            if ltp_response and ltp_response.get('status') and ltp_response.get('data') and 'ltp' in ltp_response['data']:
                live_data.append({
                    "Symbol": symbol_str,
                    "CMP": float(ltp_response['data']['ltp']),
                    "Last_Updated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                break # Success! Break out of the retry loop
                
            # 2. IF RATE LIMITED: Angel One's AB1004 error
            elif ltp_response and ltp_response.get('errorcode') == 'AB1004':
                print(f"Rate limited on {symbol_str}. Cooling down for 3s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(3)
                
            # 3. IF NULL/EMPTY OR OTHER API ERROR:
            else:
                error_msg = ltp_response.get('message', 'Unknown error') if ltp_response else 'No response from server'
                print(f"Failed/Empty data for {symbol_str}: {error_msg}. Retrying in 2s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(2)
                
        except Exception as e:
            # 4. IF NETWORK ERROR: Disconnects, timeouts
            print(f"Network error on {symbol_str}: {e}. Retrying in 2s... (Attempt {attempt+1}/{max_retries})")
            time.sleep(2)
    
    # Base rate limit delay to prevent hitting the wall
    time.sleep(0.6) 

    # Print progress to the console
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
