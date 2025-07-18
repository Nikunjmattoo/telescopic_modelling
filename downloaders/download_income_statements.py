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

def get_earnings_data(ticker_symbol, max_retries=2, delay=1):
    """Fetch earnings data for a given ticker with robust error handling.
    
    Args:
        ticker_symbol (str): The stock ticker symbol
        max_retries (int): Maximum number of retry attempts
        delay (int): Delay between retries in seconds
        
    Returns:
        dict: Earnings data if successful, None otherwise
    """
    try:
        # Clean and validate ticker symbol
        ticker_clean = ticker_symbol.upper().replace('.NS', '').strip()
        if not ticker_clean:
            print(f"Invalid ticker symbol: {ticker_symbol}")
            return None
            
        ticker = yf.Ticker(f"{ticker_clean}.NS")
        
        # Initialize data structure with metadata
        earnings_data = {
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
            earnings_data.update({
                'company_name': info.get('longName', ticker_clean),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'currency': info.get('currency', 'INR')
            })
            
            # Now try to get earnings with retry logic
            for attempt in range(max_retries):
                try:
                    # Add delay between retries
                    if attempt > 0:
                        time.sleep(delay)
                        
                    # Get income statement data (replaces deprecated earnings)
                    income_stmt = ticker.income_stmt
                    quarterly_income_stmt = ticker.quarterly_income_stmt
                    
                    # Verify we got some data
                    if (income_stmt is None or income_stmt.empty) and \
                       (quarterly_income_stmt is None or quarterly_income_stmt.empty):
                        print(f"No income statement data available for {ticker_clean}")
                        return None
                    
                    # Process annual income statement
                    if income_stmt is not None and not income_stmt.empty:
                        earnings_data['income_statement'] = convert_timestamps(income_stmt.to_dict('index'))
                        earnings_data['data_available'] = True
                    
                    # Process quarterly income statement
                    if quarterly_income_stmt is not None and not quarterly_income_stmt.empty:
                        earnings_data['quarterly_income_statement'] = convert_timestamps(quarterly_income_stmt.to_dict('index'))
                        earnings_data['data_available'] = True
                    
                    # Get additional earnings-related data
                    try:
                        earnings_dates = ticker.get_earnings_dates()
                        if earnings_dates is not None and not earnings_dates.empty:
                            earnings_data['earnings_dates'] = convert_timestamps(earnings_dates.to_dict('index'))
                    except Exception as e:
                        print(f"Could not fetch earnings dates for {ticker_clean}: {str(e)}")
                    
                    return earnings_data
                    
                except Exception as e:
                    if '404' in str(e):
                        print(f"Earnings data not available for {ticker_clean} (404)")
                        return None
                        
                    print(f"Attempt {attempt + 1} failed for {ticker_clean}: {str(e)}")
                    if attempt < max_retries - 1:
                        print(f"Retrying... (Attempt {attempt + 2}/{max_retries})")
                        continue
                    
                    print(f"Failed to fetch earnings for {ticker_clean} after {max_retries} attempts")
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
    output_dir = 'data/income_statements'
    os.makedirs(output_dir, exist_ok=True)
    
    # Load tickers
    tickers = load_tickers('c:/Projects/equity_allocator/tickers_master.txt')
    print(f"Loaded {len(tickers)} tickers")
    
    # Process each ticker
    for ticker in tqdm(tickers, desc="Downloading income statements"):
        ticker_clean = ticker.replace('.NS', '')
        output_file = os.path.join(output_dir, f'{ticker_clean}.json')
        
        # Skip if file was updated today
        if os.path.exists(output_file):
            file_time = os.path.getmtime(output_file)
            if datetime.fromtimestamp(file_time).date() == datetime.now().date():
                continue
        
        # Get earnings data
        data = get_earnings_data(ticker_clean)
        
        # Save to file
        if data and data.get('data_available'):
            with open(output_file, 'w') as f:
                json.dump(data, f, indent=2)
        
        # Be nice to the API
        time.sleep(1)
    
    print("\nIncome statements download complete!")

if __name__ == "__main__":
    main()
