#!/usr/bin/env python3
"""
Load equity tickers into database
"""
import yfinance as yf
from db_utils import DatabaseConnection

def is_equity(ticker):
    """Check if ticker is equity"""
    try:
        if not ticker.endswith('.NS'):
            ticker = f"{ticker}.NS"
        info = yf.Ticker(ticker).info
        return info.get('quoteType', '').lower() == 'equity'
    except:
        return False

def main():
    # Read tickers
    with open('nse_tickers.csv', 'r') as f:
        tickers = [line.strip() for line in f if line.strip()]
    
    print(f"Processing {len(tickers)} tickers...")
    
    # Check and insert equity tickers
    db = DatabaseConnection()
    conn = db.connect()
    cur = conn.cursor()
    
    inserted = 0
    for i, ticker in enumerate(tickers, 1):
        if is_equity(ticker):
            if not ticker.endswith('.NS'):
                ticker = f"{ticker}.NS"
            try:
                cur.execute("INSERT INTO ticker (ticker) VALUES (%s) ON CONFLICT DO NOTHING", (ticker,))
                if cur.rowcount > 0:
                    inserted += 1
                    print(f"Inserted: {ticker}")
            except Exception as e:
                print(f"Error inserting {ticker}: {e}")
        
        if i % 100 == 0:
            conn.commit()
            print(f"Processed {i}/{len(tickers)} tickers, inserted {inserted}")
    
    conn.commit()
    cur.close()
    conn.close()
    
    print(f"Done. Inserted {inserted} equity tickers.")

if __name__ == "__main__":
    main()
