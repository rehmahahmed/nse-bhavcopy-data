import pandas as pd
import yfinance as yf
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

OUTPUT_FILE = "nifty_vix_ratio_weekly.csv"

print("Downloading Nifty 50 and India VIX data since 2015...")

# Fetch data using yfinance starting from 2015
nifty = yf.download("^NSEI", start="2015-01-01", progress=False)
vix = yf.download("^INDIAVIX", start="2015-01-01", progress=False)

# Handle multi-index columns from newer yfinance versions
if isinstance(nifty.columns, pd.MultiIndex):
    nifty.columns = nifty.columns.droplevel(1)
if isinstance(vix.columns, pd.MultiIndex):
    vix.columns = vix.columns.droplevel(1)

# Extract only the Close prices and rename
nifty = nifty[['Close']].rename(columns={'Close': 'NIFTY_50'})
vix = vix[['Close']].rename(columns={'Close': 'INDIA_VIX'})

print("Merging and calculating ratio...")
# Join the two dataframes on their Date index
df = nifty.join(vix, how='inner')

# Calculate the Nifty / VIX Ratio
df['NIFTY_VIX_RATIO'] = round(df['NIFTY_50'] / df['INDIA_VIX'], 2)

# Round the raw values for cleaner dashboard presentation
df['NIFTY_50'] = round(df['NIFTY_50'], 2)
df['INDIA_VIX'] = round(df['INDIA_VIX'], 2)

print("Resampling to Weekly (Friday Close)...")
# Resample to weekly, grabbing the last available trading day of each week (handles holidays naturally)
df_weekly = df.resample('W-FRI').last().dropna().reset_index()

# Format Date to standard YYYY-MM-DD string
if 'Date' in df_weekly.columns:
    df_weekly['Date'] = df_weekly['Date'].dt.strftime('%Y-%m-%d')
else:
    # Fallback if the index reset names it differently
    df_weekly.rename(columns={'index': 'Date'}, inplace=True)
    df_weekly['Date'] = df_weekly['Date'].dt.strftime('%Y-%m-%d')

# Save to CSV
df_weekly.to_csv(OUTPUT_FILE, index=False)

print(f"✅ Success! Saved {len(df_weekly)} weeks of data to {OUTPUT_FILE}")
