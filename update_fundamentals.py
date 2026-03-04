import pandas as pd
import yfinance as yf
import numpy as np
import time
import os
import warnings
import datetime

warnings.filterwarnings('ignore')

# ==========================================
# 1. CONFIGURATION
# ==========================================
INPUT_FILE = "ind_nifty500list.csv"
OUTPUT_FILE = "nifty500_fundamentals.csv"

print(f"Loading Nifty 500 universe from {INPUT_FILE}...")
try:
    df_nifty500 = pd.read_csv(INPUT_FILE)
    if 'Symbol' in df_nifty500.columns:
        symbols = df_nifty500['Symbol'].tolist()
    else:
        symbols = df_nifty500.iloc[:, 0].tolist()
except Exception as e:
    print(f"Error reading {INPUT_FILE}: {e}")
    exit()

print(f"Fetching fundamental data for {len(symbols)} stocks.")
print("Using dynamic column fallbacks for maximum data coverage. This will take roughly 5-10 minutes...")

fundamental_data = []

# Helper function to find the first matching column alias
def get_best_column(df, aliases):
    for alias in aliases:
        if alias in df.columns:
            return alias
    return None

# ==========================================
# 2. FETCH FUNDAMENTAL DATA
# ==========================================
for i, symbol in enumerate(symbols):
    symbol = str(symbol).strip()
    yf_symbol = f"{symbol}.NS"
    
    # Set default values
    qoq_profit, qtr_profit_var, qoq_sales, opm = np.nan, np.nan, np.nan, np.nan
    
    try:
        ticker = yf.Ticker(yf_symbol)
        q_income = ticker.quarterly_income_stmt
        
        if not q_income.empty:
            q_income = q_income.T  # Transpose so Dates become rows
            
            # Define aliases to catch different reporting standards (Banks vs Manufacturing)
            rev_aliases = ['Total Revenue', 'Operating Revenue', 'Total Operating Income']
            op_inc_aliases = ['Operating Income', 'EBIT', 'Net Income Before Taxes']
            net_inc_aliases = ['Net Income', 'Net Income Applicable To Common Shares', 'Net Income From Continuing And Discontinued Operation']
            
            # Find the actual column names present for this specific stock
            rev_col = get_best_column(q_income, rev_aliases)
            op_col = get_best_column(q_income, op_inc_aliases)
            net_col = get_best_column(q_income, net_inc_aliases)
            
            # --- 1. Calculate OPM (Operating Margin) for the latest quarter ---
            if op_col and rev_col:
                op_inc = q_income[op_col].iloc[0]
                tot_rev = q_income[rev_col].iloc[0]
                if pd.notna(op_inc) and pd.notna(tot_rev) and tot_rev != 0:
                    opm = (op_inc / tot_rev) * 100
                    
            # --- 2. Calculate QoQ Sales & Profits (Latest vs Immediately Previous Quarter) ---
            if len(q_income) >= 2:
                if rev_col:
                    curr_rev = q_income[rev_col].iloc[0]
                    prev_rev = q_income[rev_col].iloc[1]
                    if pd.notna(curr_rev) and pd.notna(prev_rev) and prev_rev != 0:
                        qoq_sales = ((curr_rev / prev_rev) - 1) * 100
                        
                if net_col:
                    curr_ni = q_income[net_col].iloc[0]
                    prev_ni = q_income[net_col].iloc[1]
                    if pd.notna(curr_ni) and pd.notna(prev_ni) and prev_ni != 0:
                        qoq_profit = ((curr_ni - prev_ni) / abs(prev_ni)) * 100 
                        
            # --- 3. Calculate Qtrly Profit Variance (Latest vs Same Quarter Last Year) ---
            if len(q_income) >= 5 and net_col:
                curr_ni = q_income[net_col].iloc[0]
                yoy_ni = q_income[net_col].iloc[4] # 4 quarters ago
                if pd.notna(curr_ni) and pd.notna(yoy_ni) and yoy_ni != 0:
                    qtr_profit_var = ((curr_ni - yoy_ni) / abs(yoy_ni)) * 100

    except Exception as e:
        # Pass on catastrophic ticker failures so the loop doesn't break
        pass 

    fundamental_data.append({
        'Symbol': symbol,
        'Qtr Profit Var %': round(qtr_profit_var, 2) if pd.notna(qtr_profit_var) else np.nan,
        'QoQ profits %': round(qoq_profit, 2) if pd.notna(qoq_profit) else np.nan,
        'QoQ sales %': round(qoq_sales, 2) if pd.notna(qoq_sales) else np.nan,
        'OPM': round(opm, 2) if pd.notna(opm) else np.nan
    })

    # Print progress to the console
    if (i + 1) % 50 == 0:
        print(f"Processed {i + 1} / {len(symbols)} stocks...")
        
    time.sleep(0.1)

# ==========================================
# 3. FORMAT & SAVE TO CSV
# ==========================================
df_fund = pd.DataFrame(fundamental_data)

# Merge with Industry column from the input file
df_fund = pd.merge(df_nifty500[['Symbol', 'Industry']], df_fund, on='Symbol', how='right')

# Add Last Updated Timestamp
update_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df_fund['Last_Updated'] = update_time_str

# Clean up column order
final_cols = ['Symbol', 'Industry', 'Qtr Profit Var %', 'QoQ profits %', 'QoQ sales %', 'OPM', 'Last_Updated']
df_fund = df_fund[final_cols]

# Save to root directory
df_fund.to_csv(OUTPUT_FILE, index=False)
print(f"\n[SUCCESS] Fundamental data saved to {OUTPUT_FILE}")
print(f"Last Updated: {update_time_str}")
