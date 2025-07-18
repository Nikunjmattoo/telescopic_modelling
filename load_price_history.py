#!/usr/bin/env python3
"""
Load price history data from CSV files into database
"""
import os
import pandas as pd
from pathlib import Path
from db_utils import DatabaseConnection
from tqdm import tqdm

def get_tickers_from_db():
    """Get all tickers from database"""
    db = DatabaseConnection()
    conn = db.connect()
    cur = conn.cursor()
    
    cur.execute("SELECT ticker FROM ticker ORDER BY ticker")
    tickers = [row[0] for row in cur.fetchall()]
    
    cur.close()
    conn.close()
    
    return tickers

def load_price_data_for_ticker(ticker, data_dir):
    """Load price data from CSV for a single ticker"""
    # Remove .NS suffix for filename
    ticker_clean = ticker.replace('.NS', '')
    csv_file = data_dir / f"{ticker_clean}.csv"
    
    if not csv_file.exists():
        return None, f"CSV file not found: {csv_file}"
    
    try:
        # Read CSV - skip first 3 rows, use first column as date
        df = pd.read_csv(csv_file, skiprows=3)
        
        # The CSV has Date as first column, Close as second, etc.
        # Rename columns based on position
        expected_cols = ['date', 'close_price', 'high', 'low', 'open', 'volume', 'ticker_symbol']
        
        if len(df.columns) < 6:
            return None, f"Not enough columns: {len(df.columns)}"
            
        # Rename columns
        df.columns = expected_cols[:len(df.columns)]
        
        # Add ticker column
        df['ticker'] = ticker
        
        # Select only required columns
        required_cols = ['ticker', 'date', 'close_price', 'volume']
        available_cols = [col for col in required_cols if col in df.columns]
        
        df = df[available_cols]
        
        # Add missing columns with defaults
        if 'adjusted_close_price' not in df.columns:
            df['adjusted_close_price'] = df['close_price']  # Use close price if adj close not available
        if 'dividends' not in df.columns:
            df['dividends'] = 0.0
        
        # Handle missing columns
        for col in required_cols:
            if col not in df.columns:
                if col == 'dividends':
                    df[col] = 0.0
                elif col in ['volume']:
                    df[col] = 0
                else:
                    df[col] = None
        
        # Data cleaning - remove/normalize bad values
        # 1. Convert date and remove invalid dates
        try:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df.dropna(subset=['date'])  # Remove rows with invalid dates
            df['date'] = df['date'].dt.date
        except Exception:
            return None, "Invalid date format"
        
        # 2. Clean numeric fields - remove NaN, inf, negative prices
        numeric_fields = ['close_price', 'adjusted_close_price', 'volume', 'dividends']
        
        for field in numeric_fields:
            if field in df.columns:
                # Convert to numeric, coerce errors to NaN
                df[field] = pd.to_numeric(df[field], errors='coerce')
                
                # Handle specific field rules
                if field in ['close_price', 'adjusted_close_price']:
                    # Remove rows where price is NaN, negative, or zero
                    df = df[(df[field] > 0) & df[field].notna() & df[field].isfinite()]
                elif field == 'volume':
                    # Volume can be 0, but not negative or NaN
                    df[field] = df[field].fillna(0)
                    df = df[(df[field] >= 0) & df[field].isfinite()]
                    df[field] = df[field].astype('int64')
                elif field == 'dividends':
                    # Dividends can be 0, but not negative or NaN
                    df[field] = df[field].fillna(0.0)
                    df = df[(df[field] >= 0) & df[field].isfinite()]
        
        # 3. Remove rows with any remaining NaN in critical fields
        critical_fields = ['date', 'close_price']
        df = df.dropna(subset=critical_fields)
        
        # 4. Remove duplicate dates for same ticker
        df = df.drop_duplicates(subset=['ticker', 'date'], keep='first')
        
        # 5. Final validation - must have at least some data
        if len(df) == 0:
            return None, "No valid data after cleaning"
            
        return df, None
        
    except Exception as e:
        return None, f"Error reading CSV: {str(e)}"

def insert_price_data(df, conn):
    """Insert price data into database"""
    cur = conn.cursor()
    
    # Prepare data for insertion
    data = []
    for _, row in df.iterrows():
        data.append((
            row['ticker'],
            row['date'],
            row.get('close_price'),
            row.get('adjusted_close_price'),
            row.get('volume'),
            row.get('dividends', 0.0)
        ))
    
    # Insert with ON CONFLICT DO NOTHING to avoid duplicates
    cur.executemany("""
        INSERT INTO price_history (ticker, date, close_price, adjusted_close_price, volume, dividends, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (ticker, date) DO NOTHING
    """, data)
    
    return cur.rowcount

def main():
    # Configuration
    data_dir = Path("data/price_history")
    
    if not data_dir.exists():
        print(f"❌ Data directory not found: {data_dir}")
        return
    
    print("=== PRICE HISTORY LOADER ===\n")
    
    # Get all tickers
    print("Loading tickers from database...")
    tickers = get_tickers_from_db()
    print(f"Found {len(tickers)} tickers to process")
    
    # Connect to database
    db = DatabaseConnection()
    conn = db.connect()
    
    # Process each ticker
    processed = 0
    inserted_total = 0
    errors = []
    
    for ticker in tqdm(tickers, desc="Processing tickers"):
        # Load price data
        df, error = load_price_data_for_ticker(ticker, data_dir)
        
        if error:
            errors.append(f"{ticker}: {error}")
            continue
        
        # Insert data
        try:
            inserted = insert_price_data(df, conn)
            conn.commit()
            inserted_total += inserted
            processed += 1
            
            if processed % 100 == 0:
                print(f"Processed {processed} tickers, inserted {inserted_total} records")
                
        except Exception as e:
            conn.rollback()
            errors.append(f"{ticker}: Database error - {str(e)}")
    
    conn.close()
    
    # Print summary
    print(f"\n=== SUMMARY ===")
    print(f"Tickers processed: {processed}")
    print(f"Total records inserted: {inserted_total}")
    print(f"Errors: {len(errors)}")
    
    if errors:
        print(f"\nFirst 10 errors:")
        for error in errors[:10]:
            print(f"  {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")

if __name__ == "__main__":
    main()
