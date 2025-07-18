#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Load all tickers from nse_tickers.csv into the database
"""
import os
import sys
import time
from pathlib import Path
from tqdm import tqdm

# Set console output encoding to UTF-8
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from db_utils import DatabaseConnection

def load_tickers(file_path):
    """Load tickers from file"""
    try:
        with open(file_path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"[ERROR] Failed to load tickers: {e}")
        sys.exit(1)

def save_tickers_to_db(tickers, batch_size=100):
    """Save tickers to database in batches"""
    db = DatabaseConnection()
    conn = None
    
    try:
        conn = db.connect()
        cursor = conn.cursor()
        
        total_tickers = len(tickers)
        print(f"Starting to process {total_tickers} tickers...")
        
        # Process in batches
        for i in tqdm(range(0, total_tickers, batch_size), desc="Processing"):
            batch = tickers[i:i + batch_size]
            
            # Prepare batch insert
            values = [(ticker,) for ticker in batch]
            
            try:
                cursor.executemany(
                    """
                    INSERT INTO ticker (ticker)
                    VALUES (%s)
                    ON CONFLICT (ticker) DO NOTHING
                    """,
                    values
                )
                conn.commit()
                time.sleep(0.1)  # Small delay to avoid overwhelming the database
                
            except Exception as e:
                conn.rollback()
                print(f"\n[WARNING] Batch {i//batch_size + 1} failed: {e}")
                print("Retrying individual tickers...")
                
                # Retry failed batch one by one
                success = 0
                for ticker in batch:
                    try:
                        cursor.execute(
                            "INSERT INTO ticker (ticker) VALUES (%s) ON CONFLICT (ticker) DO NOTHING",
                            (ticker,)
                        )
                        success += 1
                    except Exception as e:
                        print(f"[ERROR] Failed to insert {ticker}: {e}")
                
                conn.commit()
                print(f"Recovered {success}/{len(batch)} tickers in failed batch")
        
        # Get final count
        cursor.execute("SELECT COUNT(*) FROM ticker")
        count = cursor.fetchone()[0]
        
        print("\n" + "="*50)
        print(f"Successfully processed {count} tickers")
        print("="*50)
        
    except Exception as e:
        print(f"\n[ERROR] Fatal error: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
        
    finally:
        if conn:
            cursor.close()
            conn.close()

def main():
    # File paths
    input_file = Path("nse_tickers.csv")
    
    if not input_file.exists():
        print(f"[ERROR] File not found: {input_file}")
        sys.exit(1)
    
    # Load tickers
    print(f"Loading tickers from {input_file}...")
    tickers = load_tickers(input_file)
    
    if not tickers:
        print("[ERROR] No tickers found in the file")
        sys.exit(1)
    
    print(f"Found {len(tickers)} tickers to process")
    
    # Save to database
    save_tickers_to_db(tickers)

if __name__ == "__main__":
    print("=== NSE TICKER LOADER ===\n")
    main()
