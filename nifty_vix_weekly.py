import pandas as pd
import yfinance as yf
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

OUTPUT_FILE = "nifty_vix_ratio_weekly.csv"

print("Downloading Nifty 50 and India VIX data...")

# Fetch from 2014 so our 52-week rolling window has enough data to start calculating cleanly in 2015
nifty = yf.download("^NSEI", start="2014-01-01", progress=False)
vix = yf.download("^INDIAVIX", start="2014-01-01", progress=False)

# Handle multi-index columns from newer yfinance versions
if isinstance(nifty.columns, pd.MultiIndex):
    nifty.columns = nifty.columns.droplevel(1)
if isinstance(vix.columns, pd.MultiIndex):
    vix.columns = vix.columns.droplevel(1)

# Extract only the Close prices and rename
nifty = nifty[['Close']].rename(columns={'Close': 'NIFTY_50'})
vix = vix[['Close']].rename(columns={'Close': 'INDIA_VIX'})

print("Merging and applying rolling oscillator math...")
df = nifty.join(vix, how='inner')

# 1. Calculate the Raw Ratio
df['RAW_RATIO'] = df['NIFTY_50'] / df['INDIA_VIX']

# 2. Resample to weekly BEFORE applying the rolling window
df_weekly = df.resample('W-FRI').last().dropna()

# 3. DE-TRENDING: Calculate a 52-week (1-year) rolling stochastic oscillator
# This forces the ratio into a strict 0 to 100 range, making it behave exactly like an RSI
rolling_min = df_weekly['RAW_RATIO'].rolling(window=52).min()
rolling_max = df_weekly['RAW_RATIO'].rolling(window=52).max()

df_weekly['RATIO_OSCILLATOR'] = ((df_weekly['RAW_RATIO'] - rolling_min) / (rolling_max - rolling_min)) * 100

# Round values for a clean dashboard UI
df_weekly['NIFTY_50'] = round(df_weekly['NIFTY_50'], 2)
df_weekly['INDIA_VIX'] = round(df_weekly['INDIA_VIX'], 2)
df_weekly['RATIO_OSCILLATOR'] = round(df_weekly['RATIO_OSCILLATOR'], 2)

# Filter the dataframe to only show 2015 onwards (hiding the 2014 math-warmup year)
df_weekly = df_weekly.loc['2015-01-01':].reset_index()

# Format Date to standard YYYY-MM-DD string
if 'Date' in df_weekly.columns:
    df_weekly['Date'] = df_weekly['Date'].dt.strftime('%Y-%m-%d')
else:
    df_weekly.rename(columns={'index': 'Date'}, inplace=True)
    df_weekly['Date'] = df_weekly['Date'].dt.strftime('%Y-%m-%d')

# Save to CSV
df_weekly.to_csv(OUTPUT_FILE, index=False)

print(f"✅ Success! Saved normalized oscillator data to {OUTPUT_FILE}")
