import os
import json
import yfinance as yf
from tqdm import tqdm
import time
from datetime import datetime
import pandas as pd

def load_tickers(ticker_file):
    """Load tickers from the master file."""
    with open(ticker_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def convert_timestamps(obj):
    """Recursively convert Timestamp objects to strings."""
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d')
    elif isinstance(obj, dict):
        return {str(k): convert_timestamps(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_timestamps(x) for x in obj]
    return obj

def get_quarterly_balance_sheet_data(ticker_symbol, max_retries=2, delay=1):
    """Fetch quarterly balance sheet data for a given ticker with robust error handling.
    
    Args:
        ticker_symbol (str): The stock ticker symbol
        max_retries (int): Maximum number of retry attempts
        delay (int): Delay between retries in seconds
        
    Returns:
        dict: Quarterly balance sheet data if successful, None otherwise
    """
    try:
        # Clean and validate ticker symbol
        ticker_clean = ticker_symbol.upper().replace('.NS', '').strip()
        if not ticker_clean:
            print(f"Invalid ticker symbol: {ticker_symbol}")
            return None
            
        ticker = yf.Ticker(f"{ticker_clean}.NS")
        
        # Initialize data structure with metadata
        quarterly_data = {
            'ticker': ticker_clean,
            'last_updated': datetime.now().isoformat(),
            'currency': 'INR',  # Default to INR for NSE stocks
            'data_available': False
        }
        
        # First verify if ticker exists and has data
        try:
            info = ticker.info
            if not info:
                print(f"No data found for {ticker_clean}")
                return None
                
            # Check if this is a valid ticker with data
            if 'symbol' not in info or not info.get('symbol'):
                print(f"Invalid ticker or no data available: {ticker_clean}")
                return None
                
            # Update with company info
            quarterly_data.update({
                'company_name': info.get('longName', ticker_clean),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'currency': info.get('currency', 'INR')
            })
            
            # Now try to get quarterly balance sheet with retry logic
            for attempt in range(max_retries):
                try:
                    # Add delay between retries
                    if attempt > 0:
                        time.sleep(delay)
                        
                    # Get quarterly balance sheet data
                    quarterly_balance_sheet = ticker.quarterly_balance_sheet
                    
                    # Verify we got some data
                    if quarterly_balance_sheet is None or quarterly_balance_sheet.empty:
                        print(f"No quarterly balance sheet data available for {ticker_clean}")
                        return None
                        
                    # Process quarterly balance sheet data
                    quarterly_data['quarterly_balance_sheet'] = convert_timestamps(quarterly_balance_sheet.to_dict('index'))
                    quarterly_data['data_available'] = True
                    return quarterly_data
                    
                except Exception as e:
                    if '404' in str(e):
                        print(f"Quarterly balance sheet data not available for {ticker_clean} (404)")
                        return None
                        
                    print(f"Attempt {attempt + 1} failed for {ticker_clean}: {str(e)}")
                    if attempt < max_retries - 1:
                        print(f"Retrying... (Attempt {attempt + 2}/{max_retries})")
                        continue
                    
                    print(f"Failed to fetch quarterly balance sheet for {ticker_clean} after {max_retries} attempts")
                    return None
                    
        except Exception as e:
            if '404' in str(e):
                print(f"Ticker not found: {ticker_clean} (404)")
            else:
                print(f"Error processing {ticker_clean}: {str(e)}")
            return None
            
        return None
        
    except Exception as e:
        print(f"Unexpected error processing {ticker_symbol}: {str(e)}")
        return None

def main():
    # Create output directory
    output_dir = 'data/quarterly_balance_sheets'
    os.makedirs(output_dir, exist_ok=True)
    
    # Load tickers
    tickers = load_tickers('c:/Projects/equity_allocator/tickers_master.txt')
    print(f"Loaded {len(tickers)} tickers")
    
    # Process each ticker
    for ticker in tqdm(tickers, desc="Downloading quarterly balance sheets"):
        ticker_clean = ticker.replace('.NS', '')
        output_file = os.path.join(output_dir, f'{ticker_clean}.json')
        
        # Skip if file was updated today
        if os.path.exists(output_file):
            file_time = os.path.getmtime(output_file)
            if datetime.fromtimestamp(file_time).date() == datetime.now().date():
                continue
        
        # Get quarterly balance sheet data
        data = get_quarterly_balance_sheet_data(ticker_clean)
        
        # Save to file
        if data and data.get('data_available'):
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
        
        # Be nice to the API
        time.sleep(1)
    
    print("\nQuarterly balance sheets download complete!")

if __name__ == "__main__":
    main()
