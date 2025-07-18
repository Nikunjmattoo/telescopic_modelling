#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clear the ticker table and load all tickers from nse_tickers.csv
"""
import sys
import time
from pathlib import Path
from tqdm import tqdm

# Set console output encoding to UTF-8
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from db_utils import DatabaseConnection

def clear_ticker_table():
    """Clear all data from the ticker table"""
    try:
        db = DatabaseConnection()
        conn = db.connect()
        cursor = conn.cursor()
        
        # Disable foreign key checks temporarily
        cursor.execute("SET session_replication_role = 'replica'")
        cursor.execute("TRUNCATE TABLE ticker CASCADE")
        cursor.execute("SET session_replication_role = 'origin'")
        conn.commit()
        
        print("✓ Cleared ticker table")
        return True
        
    except Exception as e:
        print(f"\n[ERROR] Failed to clear ticker table: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

def load_tickers_from_file(file_path):
    """Load tickers from file"""
    try:
        with open(file_path, 'r') as f:
            tickers = [line.strip() for line in f if line.strip()]
            print(f"✓ Loaded {len(tickers)} tickers from {file_path}")
            return tickers
    except Exception as e:
        print(f"[ERROR] Failed to load tickers: {e}")
        return []

def save_tickers_to_db(tickers):
    """Save tickers to database"""
    if not tickers:
        print("[WARNING] No tickers to save")
        return 0
    
    db = DatabaseConnection()
    conn = None
    total_inserted = 0
    
    try:
        conn = db.connect()
        cursor = conn.cursor()
        
        # Prepare batch insert
        batch_size = 100
        total_batches = (len(tickers) + batch_size - 1) // batch_size
        
        print(f"\nInserting {len(tickers)} tickers in {total_batches} batches...")
        
        for i in tqdm(range(0, len(tickers), batch_size), total=total_batches, desc="Processing batches"):
            batch = tickers[i:i + batch_size]
            
            # Prepare values for batch insert
            values = [(ticker,) for ticker in batch]
            
            try:
                # Insert batch
                cursor.executemany(
                    """
                    INSERT INTO ticker (ticker, is_active, created_at, last_updated)
                    VALUES (%s, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (ticker) 
                    DO UPDATE SET 
                        is_active = TRUE,
                        last_updated = CURRENT_TIMESTAMP
                    """,
                    values
                )
                
                inserted = cursor.rowcount
                total_inserted += inserted
                conn.commit()
                
                # Small delay to be nice to the database
                time.sleep(0.1)
                
            except Exception as e:
                print(f"\n[WARNING] Batch {i//batch_size + 1} failed: {e}")
                print("Retrying individual tickers...")
                
                # Retry failed batch one by one
                success = 0
                for ticker in batch:
                    try:
                        cursor.execute(
                            """
                            INSERT INTO ticker (ticker, is_active)
                            VALUES (%s, TRUE)
                            ON CONFLICT (ticker) 
                            DO UPDATE SET 
                                is_active = TRUE,
                                last_updated = CURRENT_TIMESTAMP
                            RETURNING ticker
                            """,
                            (ticker,)
                        )
                        if cursor.fetchone():
                            success += 1
                    except Exception as e:
                        print(f"[ERROR] Failed to insert {ticker}: {e}")
                
                conn.commit()
                total_inserted += success
                print(f"Recovered {success}/{len(batch)} tickers in failed batch")
        
        return total_inserted
        
    except Exception as e:
        print(f"\n[ERROR] Fatal error: {e}")
        if conn:
            conn.rollback()
        return 0
        
    finally:
        if conn:
            cursor.close()
            conn.close()

def main():
    # File paths
    input_file = Path("nse_tickers.csv")
    
    if not input_file.exists():
        print(f"[ERROR] File not found: {input_file}")
        return 1
    
    print("=== TICKER LOADER ===\n")
    
    # Step 1: Clear the ticker table
    if not clear_ticker_table():
        return 1
    
    # Step 2: Load tickers from file
    tickers = load_tickers_from_file(input_file)
    if not tickers:
        print("[ERROR] No tickers found in the file")
        return 1
    
    # Step 3: Save tickers to database
    start_time = time.time()
    inserted = save_tickers_to_db(tickers)
    
    # Print summary
    duration = time.time() - start_time
    
    print("\n" + "="*50)
    print("LOADING SUMMARY")
    print("-"*50)
    print(f"Total tickers processed: {len(tickers)}")
    print(f"Successfully inserted/updated: {inserted}")
    print(f"Time taken: {duration:.1f} seconds")
    print("="*50)
    
    return 0 if inserted > 0 else 1

if __name__ == "__main__":
    sys.exit(main())
