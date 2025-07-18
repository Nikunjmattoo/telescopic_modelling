import pandas as pd
from pathlib import Path
import yfinance as yf
from tqdm import tqdm
import time
from db_utils import DatabaseConnection

def load_tickers(file_path):
    """Load tickers from CSV file"""
    with open(file_path, 'r') as f:
        tickers = [line.strip() for line in f if line.strip()]
    return tickers

def process_and_save_tickers(tickers, max_attempts=3):
    """
    Process tickers and save directly to database
    
    Args:
        tickers: List of ticker symbols
        max_attempts: Maximum number of retry attempts
    """
    valid_count = 0
    db = DatabaseConnection()
    try:
        conn = db.connect()
        cur = conn.cursor()
        
        # Create ticker table if not exists (redundant but safe)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ticker (
                ticker VARCHAR(20) PRIMARY KEY,
                name VARCHAR(200),
                sector VARCHAR(100),
                industry VARCHAR(100),
                country VARCHAR(50),
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
            
        # Process each ticker
        for ticker in tqdm(tickers, desc="Processing tickers"):
            # Ensure ticker has .NS suffix
            if not ticker.endswith('.NS'):
                ticker = f"{ticker}.NS"
                
            for attempt in range(max_attempts):
                try:
                    # Get ticker info
                    yf_ticker = yf.Ticker(ticker)
                    info = yf_ticker.info
                
                    # Skip non-equity tickers
                    if info.get('quoteType') != 'EQUITY':
                        break
                            
                    # Prepare ticker data
                    ticker_data = {
                        'ticker': ticker,
                        'name': info.get('shortName') or info.get('longName'),
                        'sector': info.get('sector'),
                        'industry': info.get('industry'),
                        'country': info.get('country', 'India')
                    }
                        
                    # Insert or update ticker
                    cur.execute("""
                        INSERT INTO ticker (ticker, name, sector, industry, country)
                        VALUES (%(ticker)s, %(name)s, %(sector)s, %(industry)s, %(country)s)
                        ON CONFLICT (ticker) 
                        DO UPDATE SET 
                            name = EXCLUDED.name,
                            sector = EXCLUDED.sector,
                            industry = EXCLUDED.industry,
                            country = EXCLUDED.country,
                            last_updated = CURRENT_TIMESTAMP,
                            is_active = true
                        RETURNING ticker
                    """, ticker_data)
                        
                    if cur.fetchone():
                        valid_count += 1
                        if valid_count % 10 == 0:  # Commit every 10 records
                            conn.commit()
                    break
                        
                except Exception as e:
                    if attempt == max_attempts - 1:
                        print(f"Failed to process {ticker}: {str(e)}")
                    time.sleep(1)  # Rate limiting
                    continue
            
        # Final commit
        conn.commit()
        print(f"\nSuccessfully processed {valid_count} valid equity tickers")
    except Exception as e:
        print(f"Error during processing: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def main():
    # File paths
    input_file = Path("nse_tickers.csv")
    
    # Load tickers
    print("Loading tickers...")
    tickers = load_tickers(input_file)
    print(f"Loaded {len(tickers)} tickers from {input_file}")
    
    # Process and save tickers
    print("\nProcessing tickers (this may take a while)...")
    process_and_save_tickers(tickers)
    
    # Verify count
    db = DatabaseConnection()
    try:
        conn = db.connect()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM ticker")
            count = cur.fetchone()[0]
        print(f"\nTotal tickers in database: {count}")
    except Exception as e:
        print(f"Error verifying count: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
