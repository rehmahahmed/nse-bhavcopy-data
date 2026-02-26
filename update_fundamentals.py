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
    
    try:
        ticker = yf.Ticker(yf_symbol)
        info = ticker.info
        
        # Extract metrics (multiplying by 100 to convert decimals to percentages)
        qoq_profit = info.get('earningsQuarterlyGrowth')
        qoq_sales = info.get('revenueQuarterlyGrowth')
        opm = info.get('operatingMargins')
        
        fundamental_data.append({
            'Symbol': symbol,
            'Qtr Profit Var %': round(qoq_profit * 100, 2) if qoq_profit is not None else np.nan,
            'QoQ sales %': round(qoq_sales * 100, 2) if qoq_sales is not None else np.nan,
            'OPM': round(opm * 100, 2) if opm is not None else np.nan
        })
    except Exception as e:
        # If Yahoo Finance fails or is missing data for a stock, append NaNs
        fundamental_data.append({
            'Symbol': symbol, 
            'Qtr Profit Var %': np.nan, 
            'QoQ sales %': np.nan, 
            'OPM': np.nan
        })
        
    # Print progress to the GitHub Actions console
    if (i + 1) % 50 == 0:
        print(f"Processed {i + 1} / {len(symbols)} stocks...")
        
    # Sleep to prevent Yahoo Finance from blocking your IP address
    time.sleep(0.1)

# 3. Save to CSV
df_fund = pd.DataFrame(fundamental_data)

# Optional: Merge with Industry column from the input file so it's a complete standalone table
df_fund = pd.merge(df_nifty500[['Symbol', 'Industry']], df_fund, on='Symbol', how='right')

df_fund.to_csv(OUTPUT_FILE, index=False)
print(f"\n[SUCCESS] Fundamental data saved to {OUTPUT_FILE}")
