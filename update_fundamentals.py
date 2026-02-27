import pandas as pd
import yfinance as yf
import numpy as np
import time
import os
import warnings

warnings.filterwarnings('ignore')

# 1. Configuration
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

print(f"Fetching fundamental data for {len(symbols)} stocks. This will take roughly 5-10 minutes...")

fundamental_data = []

# 2. Loop through each symbol and fetch data
for i, symbol in enumerate(symbols):
    symbol = str(symbol).strip()
    yf_symbol = f"{symbol}.NS"
    
    # Set default values
    qoq_profit, qtr_profit_var, qoq_sales, opm = np.nan, np.nan, np.nan, np.nan
    
    try:
        ticker = yf.Ticker(yf_symbol)
        
        # Pull the actual quarterly income statement dataframe instead of the basic .info summary
        q_income = ticker.quarterly_income_stmt
        
        if not q_income.empty:
            # Transpose so Dates become rows (Row 0 = Latest Qtr, Row 1 = Previous Qtr, Row 4 = Same Qtr Last Year)
            q_income = q_income.T
            
            # --- 1. Calculate OPM (Operating Margin) for the latest quarter ---
            if 'Operating Income' in q_income.columns and 'Total Revenue' in q_income.columns:
                try:
                    op_inc = q_income['Operating Income'].iloc[0]
                    tot_rev = q_income['Total Revenue'].iloc[0]
                    if tot_rev and tot_rev != 0:
                        opm = (op_inc / tot_rev) * 100
                except: pass
                
            # --- 2. Calculate QoQ Sales & Profits (Latest vs Immediately Previous Quarter) ---
            if len(q_income) >= 2:
                try:
                    curr_rev = q_income['Total Revenue'].iloc[0]
                    prev_rev = q_income['Total Revenue'].iloc[1]
                    if prev_rev and prev_rev != 0:
                        qoq_sales = ((curr_rev / prev_rev) - 1) * 100
                except: pass
                
                try:
                    curr_ni = q_income['Net Income'].iloc[0]
                    prev_ni = q_income['Net Income'].iloc[1]
                    if prev_ni and prev_ni != 0:
                        # Using abs() on denominator to correctly calculate % change if profit was previously negative
                        qoq_profit = ((curr_ni - prev_ni) / abs(prev_ni)) * 100 
                except: pass
                
            # --- 3. Calculate Qtrly Profit Variance (Latest vs Same Quarter Last Year) ---
            if len(q_income) >= 5:
                try:
                    curr_ni = q_income['Net Income'].iloc[0]
                    yoy_ni = q_income['Net Income'].iloc[4] # 4 quarters ago
                    if yoy_ni and yoy_ni != 0:
                        qtr_profit_var = ((curr_ni - yoy_ni) / abs(yoy_ni)) * 100
                except: pass

    except Exception as e:
        pass # If anything fails or data is missing, the variables remain NaN safely

    fundamental_data.append({
        'Symbol': symbol,
        'Qtr Profit Var %': round(qtr_profit_var, 2) if pd.notna(qtr_profit_var) else np.nan,
        'QoQ profits %': round(qoq_profit, 2) if pd.notna(qoq_profit) else np.nan,
        'QoQ sales %': round(qoq_sales, 2) if pd.notna(qoq_sales) else np.nan,
        'OPM': round(opm, 2) if pd.notna(opm) else np.nan
    })

    # Print progress to the GitHub Actions console
    if (i + 1) % 50 == 0:
        print(f"Processed {i + 1} / {len(symbols)} stocks...")
        
    time.sleep(0.1)

# 3. Save to CSV
df_fund = pd.DataFrame(fundamental_data)

# Merge with Industry column from the input file so it's a complete standalone table
df_fund = pd.merge(df_nifty500[['Symbol', 'Industry']], df_fund, on='Symbol', how='right')

df_fund.to_csv(OUTPUT_FILE, index=False)
print(f"\n[SUCCESS] Fundamental data saved to {OUTPUT_FILE}")
