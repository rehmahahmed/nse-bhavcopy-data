import pandas as pd
import numpy as np

print("Loading daily indicators and live CMP...")

try:
    # Load the daily EOD data (generated after market close)
    # Ensure this CSV has: Symbol, Prev_Close, RSI_14, 1M_Return, 3M_Return, 6M_Return, 
    # SMA_50, SMA_200, EMA_9, ST_15_3, RS_Score, Index_ST_DIR
    daily_df = pd.read_csv('daily_rs_data.csv')
    
    # Load the live CMP data (generated 1 second ago by your other script)
    live_df = pd.read_csv('live_cmp.csv')
except FileNotFoundError as e:
    print(f"Error loading files: {e}")
    exit()

# Merge the datasets on Symbol
df = pd.merge(live_df, daily_df, on='Symbol', how='inner')

# 1. Calculate the Live 1-Day Return using CMP
# This overwrites any outdated 1-day return from the daily file
df['Live_1D_Return'] = (df['CMP'] - df['Prev_Close']) / df['Prev_Close']

# 2. Strategy Parameters & Dynamic Thresholds
is_index_down = df['Index_ST_DIR'] == 'Down'

rsi_threshold = 55
return_1d_threshold = -0.05
# Vectorized threshold for RS_Score based on Index direction
rs_score_threshold = np.where(is_index_down, 95, 80)

# 3. Apply Stock Selection Rules
stock_selection = (
    (df['RSI_14'] >= rsi_threshold) &
    (df['Live_1D_Return'] > return_1d_threshold) &
    ((df['3M_Return'] > 0.20) | (df['6M_Return'] > 0.30) | (df['1M_Return'] > 0.10)) &
    (df['SMA_50'] > df['SMA_200'])
)

# 4. Apply Buy Conditions (using Live CMP)
buy_condition = (
    (df['RS_Score'] > rs_score_threshold) &
    (df['CMP'] > df['SMA_200']) &
    (df['CMP'] > df['ST_15_3']) &
    (df['CMP'] >= df['EMA_9'] * 0.95) & 
    (df['CMP'] <= df['EMA_9'] * 1.05)
)

# Filter for valid signals
df_signals = df[stock_selection & buy_condition].copy()

print(f"Found {len(df_signals)} stocks meeting all strategy criteria.")

# 5. Select Top 6 based on RS_Score
top_allocations = df_signals.sort_values(by='RS_Score', ascending=False).head(6).copy()

# 6. Position Sizing & Stoploss Calculation
INITIAL_CAPITAL = 100000.0
MAX_POSITIONS = 6
POSITION_SIZE = INITIAL_CAPITAL / MAX_POSITIONS

if not top_allocations.empty:
    # Calculate Stoploss: 15% if Index is Down, 13% if Index is Up
    sl_multiplier = np.where(top_allocations['Index_ST_DIR'] == 'Down', 0.85, 0.87)
    
    top_allocations['Target_Entry_Price'] = top_allocations['CMP']
    top_allocations['Stop_Loss'] = (top_allocations['CMP'] * sl_multiplier).round(2)
    top_allocations['Quantity'] = (POSITION_SIZE // top_allocations['CMP']).astype(int)
    top_allocations['Invested_Value'] = top_allocations['Quantity'] * top_allocations['CMP']
    
    # Format the final output
    final_cols = ['Symbol', 'CMP', 'Live_1D_Return', 'RS_Score', 'Quantity', 'Target_Entry_Price', 'Stop_Loss', 'Invested_Value', 'Index_ST_DIR']
    output_df = top_allocations[final_cols]
    
    output_df.to_csv('target_allocations.csv', index=False)
    print(f"Successfully exported {len(output_df)} allocations to target_allocations.csv")
else:
    # Create an empty CSV with headers so PowerBI doesn't break if there are no signals
    pd.DataFrame(columns=['Symbol', 'CMP', 'Live_1D_Return', 'RS_Score', 'Quantity', 'Target_Entry_Price', 'Stop_Loss', 'Invested_Value', 'Index_ST_DIR']).to_csv('target_allocations.csv', index=False)
    print("No valid signals found. Exported empty target_allocations.csv.")
