import os
import yfinance as yf
from tqdm import tqdm
import json
from datetime import datetime
import time

def load_tickers(ticker_file):
    """Load tickers from a file."""
    with open(ticker_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def download_actions(ticker):
    """Download actions data (dividends and stock splits) for a ticker."""
    try:
        ticker_obj = yf.Ticker(ticker)
        actions = ticker_obj.actions
        
        if actions.empty:
            return None
            
        # Convert to dictionary for JSON serialization
        actions_dict = {
            'ticker': ticker,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'dividends': {},
            'splits': {}
        }
        
        # Process dividends
        if 'Dividends' in actions.columns:
            dividends = actions[actions['Dividends'] > 0]['Dividends']
            actions_dict['dividends'] = {str(date): float(div) for date, div in dividends.items()}
            
        # Process stock splits
        if 'Stock Splits' in actions.columns:
            splits = actions[actions['Stock Splits'] > 0]['Stock Splits']
            actions_dict['splits'] = {str(date): float(split) for date, split in splits.items()}
        
        return actions_dict
        
    except Exception as e:
        print(f"\nError downloading {ticker}: {str(e)}")
        return None

def main():
    # Create output directory
    output_dir = os.path.join('data', 'actions')
    os.makedirs(output_dir, exist_ok=True)
    
    # Load tickers
    tickers = load_tickers('c:/Projects/equity_allocator/tickers_master.txt')
    
    print(f"Downloading actions data for {len(tickers)} tickers...")
    
    # Process each ticker
    for ticker in tqdm(tickers, desc="Downloading"):
        ticker_clean = ticker.replace('.NS', '')
        output_file = os.path.join(output_dir, f'{ticker_clean}.json')
        
        # Skip if file already exists and was updated today
        if os.path.exists(output_file):
            file_time = datetime.fromtimestamp(os.path.getmtime(output_file))
            if file_time.date() == datetime.now().date():
                continue
        
        # Download actions data
        actions_data = download_actions(ticker)
        
        # Save to file
        if actions_data:
            with open(output_file, 'w') as f:
                json.dump(actions_data, f, indent=2)
        
        # Be nice to the API
        time.sleep(1)
    
    print("\nDownload complete!")

if __name__ == "__main__":
    main()
