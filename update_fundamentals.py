import pandas as pd
import numpy as np
import time
import os
import warnings
import datetime
import yfinance as yf

warnings.filterwarnings('ignore')

# ==========================================
# 1. CONFIGURATION
# ==========================================
INPUT_FILE = "nifty750list.csv"
OUTPUT_FILE = "nifty750_fundamentals.csv"

print(f"Loading Nifty 750 universe from {INPUT_FILE}...")
try:
    df_nifty750 = pd.read_csv(INPUT_FILE)
    df_nifty750['Symbol'] = df_nifty750['Symbol'].str.strip()
    symbols = df_nifty750['Symbol'].tolist()
except Exception as e:
    print(f"Error reading {INPUT_FILE}: {e}")
    exit(1)

print(f"Fetching fundamental data for {len(symbols)} stocks using yfinance...")
print("This will process all 750 stocks in one go. Please wait...")
fundamental_data = []

# ==========================================
# 2. FETCH FUNDAMENTAL DATA (YFINANCE)
# ==========================================
for i, symbol in enumerate(symbols):
    yf_symbol = f"{symbol}.NS"
    qoq_profit, qtr_profit_var, qoq_sales, opm = np.nan, np.nan, np.nan, np.nan
    
    try:
        ticker = yf.Ticker(yf_symbol)
        # Fetch the quarterly income statement
        inc_stmt = ticker.quarterly_income_stmt
        
        if not inc_stmt.empty and len(inc_stmt.columns) >= 2:
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
            
            # 4. YoY Quarterly Profit Var % (Needs 5 quarters of history)
            if len(inc_stmt.columns) >= 5 and 'Net Income' in inc_stmt.index:
                yoy_qtr = inc_stmt.columns[4]
                yoy_ni = inc_stmt.loc['Net Income', yoy_qtr]
                if pd.notna(curr_ni) and pd.notna(yoy_ni) and yoy_ni != 0:
                    qtr_profit_var = ((curr_ni - yoy_ni) / abs(yoy_ni)) * 100

    except Exception as e:
        # Silently pass errors (like missing data for new IPOs) to keep logs clean
        pass

    update_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fundamental_data.append({
        'Symbol': symbol,
        'Qtr Profit Var %': round(qtr_profit_var, 2) if pd.notna(qtr_profit_var) else np.nan,
        'QoQ profits %': round(qoq_profit, 2) if pd.notna(qoq_profit) else np.nan,
        'QoQ sales %': round(qoq_sales, 2) if pd.notna(qoq_sales) else np.nan,
        'OPM': round(opm, 2) if pd.notna(opm) else np.nan,
        'Last_Updated': update_time_str
    })
    
    # Small delay to be polite to Yahoo's servers
    time.sleep(0.1)
    
    if (i + 1) % 50 == 0:
        print(f"Processed {i + 1} / {len(symbols)} stocks...")

# ==========================================
# 3. MERGE & SAVE TO CSV
# ==========================================
df_new = pd.DataFrame(fundamental_data)
df_new = pd.merge(df_nifty750[['Symbol', 'Industry']], df_new, on='Symbol', how='inner')

# Clean up column order
final_cols = ['Symbol', 'Industry', 'Qtr Profit Var %', 'QoQ profits %', 'QoQ sales %', 'OPM', 'Last_Updated']
df_new = df_new[final_cols]

# Overwrite the file entirely since we fetch all 750 stocks in one go now
df_new.to_csv(OUTPUT_FILE, index=False)
print(f"\n[SUCCESS] Nifty 750 Fundamental data saved to {OUTPUT_FILE}")
