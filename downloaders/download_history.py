import yfinance as yf
import pandas as pd
from tqdm import tqdm
import os
from datetime import datetime

def load_tickers(ticker_file):
    with open(ticker_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def download_stock_data(ticker, start_date='2015-01-01'):
    try:
        # Remove .NS suffix if present for yfinance
        base_ticker = ticker.replace('.NS', '')
        data = yf.download(
            f"{base_ticker}.NS",
            start=start_date,
            progress=False
        )
        
        if not data.empty:
            data['Ticker'] = ticker
            return data
        return None
    except Exception as e:
        print(f"Error downloading {ticker}: {str(e)}")
        return None

def main():
    # Base directory for price history data
    base_dir = os.path.join('data', 'price_history')
    os.makedirs(base_dir, exist_ok=True)
    
    # Load tickers
    tickers = load_tickers('c:/Projects/equity_allocator/tickers_master.txt')
    print(f"Loaded {len(tickers)} tickers")
    
    # Create data directory if it doesn't exist
    os.makedirs(base_dir, exist_ok=True)
    
    # Download data for each ticker
    for ticker in tqdm(tickers, desc="Downloading stock data"):
        ticker_clean = ticker.replace('.NS', '')
        output_file = os.path.join(base_dir, f'{ticker_clean}.csv')
        
        # Skip if file already exists
        if os.path.exists(output_file):
            continue
            
        data = download_stock_data(ticker)
        if data is not None and not data.empty:
            data.to_csv(output_file)
    
    print("\nDownload complete!")

if __name__ == "__main__":
    main()
