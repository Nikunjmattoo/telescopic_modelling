#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Load and validate NSE equity tickers into the database
"""
import sys
import time
import yfinance as yf
from pathlib import Path
from tqdm import tqdm
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Set console output encoding to UTF-8
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from db_utils import DatabaseConnection

def setup_session():
    """Set up a requests session with retries"""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def is_equity(ticker, session=None):
    """Check if a ticker is an equity ticker using yfinance"""
    max_retries = 3
    retry_delay = 2  # seconds
    
    # Add .NS suffix if missing
    if not ticker.endswith('.NS'):
        ticker = f"{ticker}.NS"
    
    for attempt in range(max_retries):
        try:
            # Use a new session if none provided
            current_session = session or setup_session()
            
            # Get ticker object with session
            yf_ticker = yf.Ticker(ticker, session=current_session)
            
            # Try to get info with a timeout
            try:
                info = yf_ticker.info
                
                # Debug output
                print(f"\n[DEBUG] Checking {ticker}")
                print(f"quoteType: {info.get('quoteType')}")
                print(f"type: {info.get('type')}")
                print(f"symbol: {info.get('symbol')}")
                
                # Check if it's an equity
                is_eq = info.get('quoteType', '').lower() == 'equity' or info.get('type', '').lower() == 'equity'
                print(f"Is equity: {is_eq}")
                
                return is_eq
                
            except Exception as e:
                if attempt == max_retries - 1:  # Last attempt
                    print(f"\n[WARNING] Failed to fetch info for {ticker}: {str(e)}")
                    return False
                time.sleep(retry_delay)
                
        except Exception as e:
            if attempt == max_retries - 1:  # Last attempt
                print(f"\n[WARNING] Error checking {ticker}: {str(e)}")
                return False
            time.sleep(retry_delay)
    
    return False

def process_tickers(input_file, test_mode=False, limit=5):
    # Set up a single session for all requests
    session = setup_session()
    """Process tickers from file and insert valid equity tickers into database"""
    try:
        # Load tickers from file
        with open(input_file, 'r') as f:
            tickers = [line.strip() for line in f if line.strip()]
            
        if test_mode:
            tickers = tickers[:limit]
            print(f"[TEST MODE] Processing first {limit} tickers")
            
        print(f"Loaded {len(tickers)} tickers for processing")
        
        # Initialize database connection
        db = DatabaseConnection()
        conn = db.connect()
        cursor = conn.cursor()
        
        # Track statistics
        total = len(tickers)
        processed = 0
        valid_equities = 0
        skipped = 0
        errors = 0
        
        # Process tickers
        for ticker in tqdm(tickers, desc="Validating tickers"):
            try:
                # Skip if already processed (case-insensitive check)
                cursor.execute("SELECT 1 FROM ticker WHERE LOWER(ticker) = LOWER(%s)", (ticker,))
                if cursor.fetchone():
                    skipped += 1
                    processed += 1
                    continue
                
                # Check if it's an equity using the shared session
                if is_equity(ticker, session):
                    # Ensure ticker has .NS suffix
                    if not ticker.endswith('.NS'):
                        ticker = f"{ticker}.NS"
                    
                    # Insert into database
                    cursor.execute(
                        """
                        INSERT INTO ticker (ticker, is_active, created_at, last_updated)
                        VALUES (%s, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (ticker) 
                        DO UPDATE SET 
                            is_active = TRUE,
                            last_updated = CURRENT_TIMESTAMP
                        RETURNING ticker
                        """,
                        (ticker,)
                    )
                    
                    if cursor.fetchone():
                        valid_equities += 1
                        if valid_equities % 10 == 0:  # Commit every 10 valid tickers
                            conn.commit()
                else:
                    skipped += 1
                    
            except Exception as e:
                print(f"\n[ERROR] Error processing {ticker}: {e}")
                errors += 1
                conn.rollback()
            
            processed += 1
            time.sleep(1)  # Rate limiting to avoid hitting rate limits
        
        # Final commit
        conn.commit()
        
        # Print summary
        print("\n" + "="*50)
        print(f"PROCESSING SUMMARY")
        print("-"*50)
        print(f"Total tickers processed: {total}")
        print(f"Valid equity tickers: {valid_equities}")
        print(f"Skipped (non-equity/duplicate): {skipped}")
        print(f"Errors: {errors}")
        print("="*50)
        
        return valid_equities
        
    except Exception as e:
        print(f"\n[FATAL] {e}")
        return 0
        
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

def main():
    # Configuration
    input_file = Path("nse_tickers.csv")
    test_mode = True  # Set to False for full run
    test_limit = 10   # Only used if test_mode is True
    
    print("=== NSE EQUITY TICKER LOADER ===\n")
    
    if not input_file.exists():
        print(f"[ERROR] File not found: {input_file}")
        sys.exit(1)
    
    # Process tickers
    start_time = time.time()
    valid_count = process_tickers(input_file, test_mode, test_limit)
    
    # Print final status
    duration = time.time() - start_time
    print(f"\nCompleted in {duration:.1f} seconds")
    
    if test_mode:
        print("\n[NOTE] Running in TEST MODE. To process all tickers, set test_mode=False in the script.")
    
    return 0 if valid_count > 0 else 1

if __name__ == "__main__":
    sys.exit(main())
