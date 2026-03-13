import pandas as pd
import numpy as np
import urllib.request
import json
import time
import pyotp
import os
import datetime
from SmartApi import SmartConnect

# ==========================================
# 1. ANGEL ONE CREDENTIALS & LOGIN
# ==========================================
API_KEY = os.environ.get("ANGEL_API_KEY")
CLIENT_CODE = os.environ.get("ANGEL_CLIENT_CODE")
PIN = os.environ.get("ANGEL_PIN")
TOTP_SECRET = os.environ.get("ANGEL_TOTP_SECRET")

print("Logging into Angel One...")
smartApi = SmartConnect(api_key=API_KEY)
totp = pyotp.TOTP(TOTP_SECRET).now()
login_data = smartApi.generateSession(CLIENT_CODE, PIN, totp)

if not login_data['status']:
    print("Login Failed:", login_data['message'])
    exit()

# Fetch token map
instrument_url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
response = urllib.request.urlopen(instrument_url)
instrument_list = json.loads(response.read())
token_map = {inst['symbol'].replace('-EQ', ''): inst['token'] 
             for inst in instrument_list if inst['exch_seg'] == 'NSE' and inst['symbol'].endswith('-EQ')}

# ==========================================
# 2. FETCH LIVE DATA FROM ANGEL ONE
# ==========================================
# Load the master list of stocks to scan
df_nifty = pd.read_csv('nifty750list.csv')
symbols_to_scan = df_nifty['Symbol'].tolist()

live_data = []
print(f"Fetching live prices for {len(symbols_to_scan)} stocks...")

for i, symbol in enumerate(symbols_to_scan):
    symbol_str = str(symbol).strip()
    if symbol_str not in token_map: continue
    
    for attempt in range(3):
        try:
            ltp_response = smartApi.ltpData("NSE", f"{symbol_str}-EQ", token_map[symbol_str])
            if ltp_response and ltp_response.get('status') and ltp_response.get('data'):
                cmp = float(ltp_response['data']['ltp'])
                prev_close = float(ltp_response['data']['close'])
                
                # Live 1D Return Calculation
                one_day_return = ((cmp - prev_close) / prev_close) if prev_close > 0 else 0.0
                
                live_data.append({
                    "Symbol": symbol_str,
                    "CMP": cmp,
                    "Prev_Close": prev_close,
                    "Live_1D_Return": one_day_return
                })
                break 
            elif ltp_response and ltp_response.get('errorcode') == 'AB1004':
                time.sleep(2)
            else:
                break
        except Exception:
            time.sleep(1)
            
    time.sleep(0.4) # Respect 3 requests/sec rate limit
    if (i + 1) % 100 == 0:
        print(f"  Processed {i + 1} stocks...")

df_live = pd.DataFrame(live_data)

# ==========================================
# 3. MERGE LIVE DATA WITH EOD INDICATORS
# ==========================================
print("Merging live data with daily indicators...")
try:
    daily_df = pd.read_csv('daily_rs_data.csv')
    # Clean symbol in daily_df if it has '.NS' suffix
    daily_df['Symbol'] = daily_df['Symbol'].str.replace('.NS', '', regex=False)
except FileNotFoundError:
    print("Error: daily_rs_data.csv not found. Please run your EOD indicator scanner first.")
    exit()

df = pd.merge(df_live, daily_df, on='Symbol', how='inner')

# ==========================================
# 4. LOAD PORTFOLIO STATE (THE MEMORY)
# ==========================================
STATE_FILE = 'portfolio_state.json'
MAX_POSITIONS = 6
INITIAL_CAPITAL = 100000.0

if os.path.exists(STATE_FILE):
    with open(STATE_FILE, 'r') as f:
        state = json.load(f)
else:
    print("No portfolio state found. Initializing fresh portfolio.")
    state = {'cash': INITIAL_CAPITAL, 'positions': {}}

def get_total_equity(current_state, live_df):
    equity = current_state['cash']
    for sym, pos in current_state['positions'].items():
        row = live_df[live_df['Symbol'] == sym]
        if not row.empty:
            equity += pos['qty'] * row['CMP'].values[0]
        else:
            equity += pos['qty'] * pos['entry_price']
    return equity

# ==========================================
# 5. EVALUATE EXITS (SELL RULES)
# ==========================================
positions = state['positions']
print("\nChecking for Sell Signals...")

for sym, pos in list(positions.items()):
    row = df[df['Symbol'] == sym]
    if row.empty: continue
    
    cmp = row['CMP'].values[0]
    prev_close = row['Prev_Close'].values[0]  # This is yesterday's close from Angel
    st_15_3 = row['ST_15_3'].values[0]
    sma_200 = row['SMA_200'].values[0]
    
    sell_reason = None
    
    # EOD Sells (Based on yesterday's closing price vs yesterday's indicators)
    if prev_close < st_15_3:
        sell_reason = "Close < ST(15,3)"
    elif prev_close < sma_200:
        sell_reason = "Close < 200 SMA"
    # Intraday Stoploss (Based on live CMP)
    elif cmp <= pos['sl_price']:
        sell_reason = "Intraday SL Hit"
        
    if sell_reason:
        print(f"  🔴 SELL: {sym} at ₹{cmp} | Reason: {sell_reason}")
        state['cash'] += (pos['qty'] * cmp)
        del positions[sym]

# ==========================================
# 6. EVALUATE ENTRIES (BUY RULES)
# ==========================================
open_slots = MAX_POSITIONS - len(positions)
print(f"\nOpen Portfolio Slots: {open_slots}")

if open_slots > 0:
    is_index_down = df['Index_ST_DIR'] == 'Down'
    rsi_threshold = 55
    return_1d_threshold = -0.05
    rs_score_threshold = np.where(is_index_down, 95, 80)

    stock_selection = (
        (df['RSI_14'] >= rsi_threshold) &
        (df['Live_1D_Return'] > return_1d_threshold) &
        ((df['3M_Return'] > 0.20) | (df['6M_Return'] > 0.30) | (df['1M_Return'] > 0.10)) &
        (df['SMA_50'] > df['SMA_200'])
    )

    buy_condition = (
        (df['RS_Score'] > rs_score_threshold) &
        (df['CMP'] > df['SMA_200']) &
        (df['CMP'] > df['ST_15_3']) &
        (df['CMP'] >= df['EMA_9'] * 0.95) & 
        (df['CMP'] <= df['EMA_9'] * 1.05)
    )

    not_held = ~df['Symbol'].isin(positions.keys())
    
    df_signals = df[stock_selection & buy_condition & not_held].copy()
    new_buys = df_signals.sort_values(by='RS_Score', ascending=False).head(open_slots)
    
    for _, row in new_buys.iterrows():
        sym = row['Symbol']
        cmp = row['CMP']
        idx_dir = row['Index_ST_DIR']
        
        total_equity = get_total_equity(state, df)
        target_allocation = total_equity / MAX_POSITIONS
        
        invest_amount = min(state['cash'], target_allocation)
        qty = int(invest_amount // cmp)
        
        if qty > 0:
            cost = qty * cmp
            state['cash'] -= cost
            sl_multiplier = 0.85 if idx_dir == 'Down' else 0.87
            
            positions[sym] = {
                'entry_date': datetime.datetime.now().strftime("%Y-%m-%d"),
                'entry_price': float(cmp),
                'qty': int(qty),
                'sl_price': round(float(cmp) * sl_multiplier, 2)
            }
            print(f"  🟢 BUY: {sym} | Qty: {qty} | Entry: ₹{cmp}")

# ==========================================
# 7. EXPORT STATE & POWER BI DATA
# ==========================================
with open(STATE_FILE, 'w') as f:
    json.dump(state, f, indent=4)

total_equity = get_total_equity(state, df)
export_data = []

for sym, pos in positions.items():
    row = df[df['Symbol'] == sym]
    cmp = row['CMP'].values[0] if not row.empty else pos['entry_price']
    live_1d = row['Live_1D_Return'].values[0] if not row.empty else 0.0
    rs_score = row['RS_Score'].values[0] if not row.empty else 0
    idx_dir = row['Index_ST_DIR'].values[0] if not row.empty else "Up"
    
    live_value = pos['qty'] * cmp
    invest_pct = (live_value / total_equity) * 100
    
    export_data.append({
        'Symbol': sym,
        'CMP': cmp,
        'Live_1D_Return': round(live_1d, 4),
        'RS_Score': rs_score,
        'Quantity': pos['qty'],
        'Target_Entry_Price': pos['entry_price'],
        'Stop_Loss': pos['sl_price'],
        'Investment_Percentage': round(invest_pct, 2),
        'Index_ST_DIR': idx_dir
    })

cash_pct = (state['cash'] / total_equity) * 100
export_data.append({
    'Symbol': 'CASH_RESERVE',
    'CMP': 1.0,
    'Live_1D_Return': 0.0,
    'RS_Score': 0,
    'Quantity': round(state['cash'], 2),
    'Target_Entry_Price': 1.0,
    'Stop_Loss': 0.0,
    'Investment_Percentage': round(cash_pct, 2),
    'Index_ST_DIR': 'N/A'
})

output_df = pd.DataFrame(export_data)
output_df.to_csv('target_allocations.csv', index=False)

print("\n" + "="*50)
print(f"Total Portfolio Value: ₹{total_equity:,.2f} | Cash: ₹{state['cash']:,.2f}")
print(f"Successfully exported target_allocations.csv for PowerBI.")
print("="*50)

# =========================================================================
# --- BATON PASS: EXPORT FINAL STATE FOR LIVE EXECUTION SCRIPT ---
# =========================================================================
import json

print("\nGenerating baton-pass state file for live execution...")

final_portfolio_state = {
    "cash": round(capital, 2),
    "positions": {}
}

# last_day_data is already defined in your backtest's final portfolio mutation logic
for ticker, pos in positions.items():
    # Skip pending buys; the live script's scanner will natively pick them up tomorrow anyway
    if pos['raw_entry_price'] == 'Will update when market opens':
        continue
        
    # Get the most recent Index trend for Stoploss calculation
    idx_dir = last_day_data.loc[ticker, 'Index_ST_DIR'] if ticker in last_day_data.index else 'Up'
    sl_multiplier = 0.85 if idx_dir == 'Down' else 0.87
    
    # Strip the '.NS' suffix so it matches the live script's symbol format perfectly
    clean_symbol = ticker.replace('.NS', '')

    final_portfolio_state["positions"][clean_symbol] = {
        "entry_date": str(pos['entry_date'])[:10],
        "entry_price": round(pos['raw_entry_price'], 2),
        "qty": pos['qty'],
        "sl_price": round(pos['raw_entry_price'] * sl_multiplier, 2)
    }

# Save it to the same directory your live script runs from
state_file_path = 'portfolio_state.json'
with open(state_file_path, 'w') as f:
    json.dump(final_portfolio_state, f, indent=4)

print(f"[SUCCESS] Saved 5-year backtest ending state to {state_file_path}!")
print(f"Carry-forward Cash: ₹{round(capital, 2)}")
print(f"Carry-forward Positions: {list(final_portfolio_state['positions'].keys())}")
print("Your live script is now fully primed with 5 years of historical compounding.")
