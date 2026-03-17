import pandas as pd
import yfinance as yf
import os
import time
from datetime import datetime, timedelta
from transformers import pipeline
from gnews import GNews
import warnings

warnings.filterwarnings('ignore')

CSV_FILENAME = "fear_greed_master.csv"

# 1. LOAD HISTORY & DETERMINE DATES
end_date = datetime.now()

if os.path.exists(CSV_FILENAME):
    print(f"Loading existing database: {CSV_FILENAME}")
    df_history = pd.read_csv(CSV_FILENAME)
    
    # NEW: Force all column names to lowercase to fix the 'Date' vs 'date' issue
    df_history.columns = df_history.columns.str.lower()
    
    # Safety catch: If the index was saved without a name, pandas calls it 'unnamed: 0'
    if 'date' not in df_history.columns and 'unnamed: 0' in df_history.columns:
        df_history = df_history.rename(columns={'unnamed: 0': 'date'})
        
    df_history['date'] = pd.to_datetime(df_history['date'])
    last_date = df_history['date'].max()
else:
    print("No history found. Initializing with the last 30 days of news...")
    df_history = pd.DataFrame()
    start_date = end_date - timedelta(days=30)

# 2. FETCH LATEST NEWS
print(f"Scraping news from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
google_news = GNews(
    language='en', country='IN',
    start_date=(start_date.year, start_date.month, start_date.day),
    end_date=(end_date.year, end_date.month, end_date.day),
    max_results=50
)

queries = ['Nifty+500', 'Indian+stock+market', 'NSE+India']
all_articles = []

for query in queries:
    try:
        articles = google_news.get_news(query)
        all_articles.extend(articles)
        time.sleep(2)
    except Exception as e:
        print(f"Failed to fetch {query}: {e}")

if not all_articles:
    print("No new articles found today. Exiting.")
    exit()

df_new = pd.DataFrame(all_articles)
df_new = df_new.rename(columns={'title': 'headline', 'published date': 'date'})
df_new['date'] = pd.to_datetime(df_new['date'], format='mixed', errors='coerce').dt.date
df_new = df_new.dropna(subset=['headline', 'date'])

# 3. SCORE NEW ARTICLES
print("Loading FinBERT for Sentiment Analysis...")
sentiment_analyzer = pipeline("sentiment-analysis", model="ProsusAI/finbert")

scores = []
for result in sentiment_analyzer(df_new["headline"].tolist(), batch_size=16):
    if result["label"] == "positive": scores.append(1.0)
    elif result["label"] == "negative": scores.append(-1.0)
    else: scores.append(0.0)

df_new["sentiment_score"] = scores
daily_sentiment = df_new.groupby("date")["sentiment_score"].mean().reset_index()
daily_sentiment['date'] = pd.to_datetime(daily_sentiment['date'])

# 4. MERGE WITH HISTORY & PRICE DATA
if not df_history.empty:
    daily_sentiment = pd.concat([df_history[['date', 'sentiment_score']], daily_sentiment])
    daily_sentiment = daily_sentiment.groupby('date').mean().reset_index()

daily_sentiment.set_index("date", inplace=True)

print("Fetching Nifty 500 Price Data...")
fetch_start = daily_sentiment.index.min()
price_df = yf.Ticker("^CRSLDX").history(start=fetch_start)
price_df.index = pd.to_datetime(price_df.index).normalize()
if price_df.index.tz is not None:
    price_df.index = price_df.index.tz_localize(None)

master_df = price_df[["Close"]].join(daily_sentiment, how="left")
master_df["sentiment_score"] = master_df["sentiment_score"].ffill().fillna(0)

# 5. CALCULATE FEAR & GREED INDEX
master_df["smooth_sentiment"] = master_df["sentiment_score"].rolling(window=3, min_periods=1).mean()
master_df["fear_greed_index"] = ((master_df["smooth_sentiment"] - (-1)) / 2) * 100
master_df["fear_greed_index"] = master_df["fear_greed_index"].round(0)

master_df.reset_index(inplace=True)
master_df.rename(columns={'index': 'date', 'Date': 'date'}, inplace=True)

master_df.to_csv(CSV_FILENAME, index=False)
print(f"✅ Success! Saved {len(master_df)} days of sentiment data to {CSV_FILENAME}")
