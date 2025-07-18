#!/usr/bin/env python3
"""
Load price history data from CSV files into database
CSV Format: Date,Close,High,Low,Open,Volume,Ticker (data starts row 4)
"""
import pandas as pd
import numpy as np
from pathlib import Path
from db_utils import DatabaseConnection
from tqdm import tqdm

def load_price_data_for_ticker(ticker, data_dir):
    """Load price data from CSV for a single ticker"""
    # Remove .NS suffix for filename
    ticker_clean = ticker.replace('.NS', '')
    csv_file = data_dir / f"{ticker_clean}.csv"
    
    if not csv_file.exists():
        return None, f"CSV file not found"
    
    try:
        # Read CSV - skip first 3 rows, data starts from row 4
        df = pd.read_csv(csv_file, skiprows=3, header=None)
        
        # Set column names based on actual format
        df.columns = ['date', 'close_price', 'high', 'low', 'open', 'volume', 'ticker_symbol']
        
        # Keep only required columns and add our ticker
        df = df[['date', 'close_price', 'volume']].copy()
        df['ticker'] = ticker
        df['adjusted_close_price'] = df['close_price']  # Use close as adjusted close
        df['dividends'] = 0.0
        
        # Clean data - remove invalid entries
        # 1. Convert date
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        df['date'] = df['date'].dt.date
        
        # 2. Clean prices - must be positive numbers
        df['close_price'] = pd.to_numeric(df['close_price'], errors='coerce')
        df['adjusted_close_price'] = pd.to_numeric(df['adjusted_close_price'], errors='coerce')
        df = df[(df['close_price'] > 0) & (df['adjusted_close_price'] > 0)]
        
        # 3. Clean volume - must be non-negative integer
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
        df = df[df['volume'] >= 0]
        df['volume'] = df['volume'].astype(int)
        
        # 4. Remove duplicates by date
        df = df.drop_duplicates(subset=['date'], keep='first')
        
        # 5. Final check
        if len(df) == 0:
            return None, "No valid data after cleaning"
            
        return df, None
        
    except Exception as e:
        return None, f"Error reading CSV: {str(e)}"

def main():
    # Get tickers from database
    db = DatabaseConnection()
    conn = db.connect()
    cur = conn.cursor()
    cur.execute("SELECT ticker FROM ticker ORDER BY ticker")
    tickers = [row[0] for row in cur.fetchall()]
    cur.close()
    
    print(f"Processing {len(tickers)} tickers...")
    
    data_dir = Path("data/price_history")
    processed = 0
    inserted_total = 0
    errors = []
    
    for ticker in tqdm(tickers, desc="Loading price data"):
        df, error = load_price_data_for_ticker(ticker, data_dir)
        
        if error:
            errors.append(f"{ticker}: {error}")
            continue
        
        # Insert data
        try:
            cur = conn.cursor()
            data = []
            for _, row in df.iterrows():
                data.append((
                    row['ticker'],
                    row['date'],
                    float(row['close_price']),
                    float(row['adjusted_close_price']),
                    int(row['volume']),
                    float(row['dividends'])
                ))
            
            cur.executemany("""
                INSERT INTO price_history (ticker, date, close_price, adjusted_close_price, volume, dividends, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (ticker, date) DO NOTHING
            """, data)
            
            inserted = cur.rowcount
            conn.commit()
            cur.close()
            
            inserted_total += inserted
            processed += 1
            
        except Exception as e:
            conn.rollback()
            errors.append(f"{ticker}: Database error - {str(e)}")
    
    conn.close()
    
    print(f"\n=== SUMMARY ===")
    print(f"Processed: {processed}/{len(tickers)} tickers")
    print(f"Records inserted: {inserted_total}")
    print(f"Errors: {len(errors)}")
    
    if errors and len(errors) <= 10:
        print("\nErrors:")
        for error in errors:
            print(f"  {error}")

if __name__ == "__main__":
    main()
