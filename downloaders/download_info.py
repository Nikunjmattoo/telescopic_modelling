import yfinance as yf
import pandas as pd
from tqdm import tqdm
import os
import json
from datetime import datetime

def load_tickers(ticker_file):
    """Load tickers from a text file."""
    with open(ticker_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def get_company_info(ticker):
    """Fetch company information using yfinance."""
    try:
        # Remove .NS suffix if present for yfinance
        base_ticker = ticker.replace('.NS', '')
        ticker_obj = yf.Ticker(f"{base_ticker}.NS")
        
        # Get company info
        info = ticker_obj.info
        
        # Add timestamp
        info['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return info
    except Exception as e:
        print(f"Error fetching info for {ticker}: {str(e)}")
        return None

def main():
    # Base directory for company info
    base_dir = os.path.join('data', 'info')
    os.makedirs(base_dir, exist_ok=True)
    print(f"Saving company info to: {os.path.abspath(base_dir)}")
    
    # Load tickers
    tickers = load_tickers('c:/Projects/equity_allocator/tickers_master.txt')
    print(f"Loaded {len(tickers)} tickers")
    
    # Create data directory if it doesn't exist
    os.makedirs(base_dir, exist_ok=True)
    
    # Download info for each ticker
    for ticker in tqdm(tickers, desc="Downloading company info"):
        ticker_clean = ticker.replace('.NS', '')
        output_file = os.path.join(base_dir, f'{ticker_clean}.json')
        
        # Skip if file already exists and is recent (within last 7 days)
        if os.path.exists(output_file):
            file_time = os.path.getmtime(output_file)
            if (time.time() - file_time) < (7 * 24 * 60 * 60):  # 7 days in seconds
                print(f"Skipping {ticker_clean} - already downloaded recently")
                continue
        
        info = get_company_info(ticker)
        if info:
            with open(output_file, 'w') as f:
                json.dump(info, f, indent=2)
    
    print("\nCompany info download complete!")

if __name__ == "__main__":
    main()
