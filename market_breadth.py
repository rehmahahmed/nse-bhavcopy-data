import pandas as pd
import datetime
import yfinance as yf
import warnings

warnings.filterwarnings('ignore')

# ==========================================
# 1. CONFIGURATION
# ==========================================
INPUT_FILE = "nifty750list.csv"
OUTPUT_FILE = "market_breadth_history_5yr.csv"

# --- 2015 Time Calculation with DMA Padding ---
target_start_date = datetime.datetime(2015, 1, 1)

# We need 200 trading days (~300 calendar days) BEFORE our 2015 start date 
# so the 200 SMA can calculate properly on day one (January 2015).
start_date = target_start_date - datetime.timedelta(days=300)

# ==========================================
# 2. LOAD SYMBOLS & INDUSTRY MAPPING
# ==========================================
try:
    df_tickers = pd.read_csv(INPUT_FILE)
    df_tickers['Symbol'] = df_tickers['Symbol'].str.strip()
    symbols = df_tickers['Symbol'].tolist()
    
    # Create a dictionary mapping each symbol to its Industry
    industry_map = dict(zip(df_tickers['Symbol'], df_tickers['Industry']))
except Exception as e:
    print(f"Error reading {INPUT_FILE}: {e}")
    exit()

# Format symbols for Yahoo Finance (add .NS for NSE)
yf_tickers = [f"{sym}.NS" for sym in symbols]

# ==========================================
# 3. FETCH HISTORICAL DATA IN BULK
# ==========================================
print(f"Fetching history since 2014 for {len(yf_tickers)} stocks via YFinance...")
print("This takes roughly 1 to 2 minutes. Please wait...")

# yfinance bulk download is vastly faster and prevents missing data chunks
raw_data = yf.download(yf_tickers, start=start_date.strftime("%Y-%m-%d"), progress=False, auto_adjust=False)

if raw_data.empty:
    print("No data fetched. Exiting.")
    exit()

# Extract only the "Close" prices from the MultiIndex dataframe
if isinstance(raw_data.columns, pd.MultiIndex):
    df_close = raw_data['Close']
else:
    df_close = raw_data[['Close']]

# Strip the '.NS' suffix so column names match our industry_map exactly
df_close.columns = [str(col).replace('.NS', '') for col in df_close.columns]

# ==========================================
# 4. CALCULATE HISTORICAL METRICS
# ==========================================
print("Calculating the 200 SMA...")
# Forward fill NA values slightly to prevent 1-day API glitches from zeroing out breadth
df_close = df_close.ffill()

# Calculate the 200 Simple Moving Average for the entire matrix
sma_200 = df_close.rolling(window=200).mean()

# ==========================================
# 5. AGGREGATE COUNTS & SAVE (EXACTLY FROM 2015)
# ==========================================
print("Aggregating breadth metric by Nifty 750 and Sectors...")

# Create an empty dataframe with our Dates as the index
df_breadth = pd.DataFrame(index=df_close.index)

fetched_symbols = df_close.columns.tolist()

# 1. Master Breadth Count
df_breadth['Total_Above_200_SMA'] = (df_close > sma_200).sum(axis=1)

# 2. Sector-Specific Breadth
industry_groups = {}
for sym in fetched_symbols:
    ind = industry_map.get(sym, 'Unknown Sector')
    if pd.isna(ind): ind = 'Unknown Sector'
    
    if ind not in industry_groups:
        industry_groups[ind] = []
    industry_groups[ind].append(sym)

for ind, syms in industry_groups.items():
    # Filter to only the symbols that actually successfully downloaded
    valid_syms = [s for s in syms if s in df_close.columns]
    if valid_syms:
        df_breadth[ind] = (df_close[valid_syms] > sma_200[valid_syms]).sum(axis=1)
    else:
        df_breadth[ind] = 0

# Slice the dataframe to exactly the 2015 mark
cutoff_date_str = target_start_date.strftime('%Y-%m-%d')
df_breadth = df_breadth.loc[cutoff_date_str:]

# Convert the Date index back into a standard column for merging
df_breadth = df_breadth.reset_index()
df_breadth['Date'] = df_breadth['Date'].dt.strftime('%Y-%m-%d')

# --- NEW: FETCH AND MERGE INDEX DATA ---
print("Fetching Index data to overlay with breadth...")
# Trying a 750 proxy first, falling back to standard 500
index_ticker = "NIFTY_750.NS" 
idx_data = yf.download(index_ticker, start=cutoff_date_str, progress=False, auto_adjust=False)

if idx_data.empty:
    print(f"{index_ticker} not found. Falling back to Nifty 500 (^CRSLDX) for benchmark correlation...")
    idx_data = yf.download("^CRSLDX", start=cutoff_date_str, progress=False, auto_adjust=False)

if not idx_data.empty:
    if isinstance(idx_data.columns, pd.MultiIndex):
        idx_data.columns = idx_data.columns.droplevel(1)
        
    idx_data = idx_data.reset_index()
    idx_data['Date'] = pd.to_datetime(idx_data['Date']).dt.strftime('%Y-%m-%d')
    idx_data = idx_data[['Date', 'Close']].rename(columns={'Close': 'Index_Close'})
    
    # Merge on the exact dates we have breadth data for
    df_breadth = pd.merge(df_breadth, idx_data, on='Date', how='left')
    
    # Forward-fill index prices in case the API missed a specific holiday
    df_breadth['Index_Close'] = df_breadth['Index_Close'].ffill()
else:
    print("Warning: Could not fetch index data.")
    df_breadth['Index_Close'] = None
# ---------------------------------------

df_breadth.to_csv(OUTPUT_FILE, index=False)

print(f"\n[SUCCESS] Generated clean breadth history (Starting {cutoff_date_str}).")
print(f"Saved to {OUTPUT_FILE}")
print(f"Total trading days recorded: {len(df_breadth)}")
