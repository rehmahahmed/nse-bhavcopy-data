0.i2mport pandas as pd
import numpy as np
import time
import os
import warnings
import datetime
import requests

warnings.filterwarnings('ignore')

# ==========================================
# 1. CONFIGURATION
# ==========================================
INPUT_FILE = "nifty750list.csv"
OUTPUT_FILE = "nifty750_fundamentals.csv"
FMP_API_KEY = os.environ.get("FMP_API_KEY")

if not FMP_API_KEY:
    print("Error: FMP_API_KEY environment variable not found. Exiting.")
    exit(1)

print(f"Loading Nifty 750 universe from {INPUT_FILE}...")
try:
    df_nifty750 = pd.read_csv(INPUT_FILE)
    if 'Symbol' in df_nifty750.columns:
        symbols = df_nifty750['Symbol'].tolist()
    else:
        symbols = df_nifty750.iloc[:, 0].tolist()
except Exception as e:
    print(f"Error reading {INPUT_FILE}: {e}")
    exit(1)

# --- CHUNK LOGIC FOR FREE TIER (3-Day Cycle for 750 stocks) ---
# CHUNK_INDEX should be 0, 1, or 2 (Set this in GitHub Actions)
CHUNK_INDEX = int(os.environ.get("CHUNK_INDEX", 0))
CHUNK_SIZE = 250
start_idx = CHUNK_INDEX * CHUNK_SIZE
end_idx = start_idx + CHUNK_SIZE

# Slice the list to process 250 stocks at a time
target_symbols = symbols[start_idx:end_idx]

print(f"Processing Chunk {CHUNK_INDEX}: Fetching {len(target_symbols)} stocks (Index {start_idx} to {end_idx}).")

fundamental_data = []

import yfinance as yf

# ... (keep your setup and chunking logic) ...

for i, symbol in enumerate(target_symbols):
    symbol = str(symbol).strip()
    yf_symbol = f"{symbol}.NS"
    
    qoq_profit, qtr_profit_var, qoq_sales, opm = np.nan, np.nan, np.nan, np.nan
    
    try:
        ticker = yf.Ticker(yf_symbol)
        # Fetch the quarterly income statement
        inc_stmt = ticker.quarterly_income_stmt
        
        if not inc_stmt.empty and len(inc_stmt.columns) >= 2:
            # yfinance returns dates as columns, sorted newest to oldest
            latest_qtr = inc_stmt.columns[0]
            prev_qtr = inc_stmt.columns[1]
            
            # 1. Operating Profit Margin (OPM)
            if 'Operating Income' in inc_stmt.index and 'Total Revenue' in inc_stmt.index:
                op_inc = inc_stmt.loc['Operating Income', latest_qtr]
                tot_rev = inc_stmt.loc['Total Revenue', latest_qtr]
                if pd.notna(op_inc) and pd.notna(tot_rev) and tot_rev != 0:
                    opm = (op_inc / tot_rev) * 100

            # 2. QoQ Sales %
            if 'Total Revenue' in inc_stmt.index:
                curr_rev = inc_stmt.loc['Total Revenue', latest_qtr]
                prev_rev = inc_stmt.loc['Total Revenue', prev_qtr]
                if pd.notna(curr_rev) and pd.notna(prev_rev) and prev_rev != 0:
                    qoq_sales = ((curr_rev / prev_rev) - 1) * 100

            # 3. QoQ Profit %
            if 'Net Income' in inc_stmt.index:
                curr_ni = inc_stmt.loc['Net Income', latest_qtr]
                prev_ni = inc_stmt.loc['Net Income', prev_qtr]
                if pd.notna(curr_ni) and pd.notna(prev_ni) and prev_ni != 0:
                    qoq_profit = ((curr_ni - prev_ni) / abs(prev_ni)) * 100
            
            # 4. YoY Quarterly Profit Var % (Needs at least 5 quarters of data)
            if len(inc_stmt.columns) >= 5 and 'Net Income' in inc_stmt.index:
                yoy_qtr = inc_stmt.columns[4] # The quarter from exactly 1 year ago
                yoy_ni = inc_stmt.loc['Net Income', yoy_qtr]
                if pd.notna(curr_ni) and pd.notna(yoy_ni) and yoy_ni != 0:
                    qtr_profit_var = ((curr_ni - yoy_ni) / abs(yoy_ni)) * 100

    except Exception as e:
        print(f"Error fetching {symbol} via yfinance: {e}")
        pass

# ... (keep your append and CSV saving logic) ...
# ==========================================
# 3. MERGE & SAVE TO CSV
# ==========================================
df_new = pd.DataFrame(fundamental_data)
# Merge with original Industry labels
df_new = pd.merge(df_nifty750[['Symbol', 'Industry']], df_new, on='Symbol', how='inner')

if os.path.exists(OUTPUT_FILE):
    print(f"Found existing {OUTPUT_FILE}. Merging new chunk data...")
    df_old = pd.read_csv(OUTPUT_FILE)
    
    # Use Symbol as index for the update
    df_old.set_index('Symbol', inplace=True)
    df_new.set_index('Symbol', inplace=True)
    
    # Update existing rows with fresh data
    df_old.update(df_new)
    
    # Combine with any brand new symbols
    new_symbols = df_new[~df_new.index.isin(df_old.index)]
    df_final = pd.concat([df_old, new_symbols]).reset_index()
else:
    print(f"No existing {OUTPUT_FILE} found. Creating new...")
    df_final = df_new

# Maintain consistent column order
final_cols = ['Symbol', 'Industry', 'Qtr Profit Var %', 'QoQ profits %', 'QoQ sales %', 'OPM', 'Last_Updated']

for col in final_cols:
    if col not in df_final.columns:
        df_final[col] = np.nan

df_final = df_final[final_cols]
df_final.to_csv(OUTPUT_FILE, index=False)
print(f"\n[SUCCESS] Nifty 750 Fundamental data saved to {OUTPUT_FILE}")
