import os
import json
import time
import yfinance as yf
from datetime import datetime
from tqdm import tqdm

# Configuration
OUTPUT_DIR = 'data/quarterly_income_statements'
TICKERS_FILE = 'c:/Projects/equity_allocator/tickers_master.txt'

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

def safe_serialize(obj):
    """Convert non-serializable objects to strings."""
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    elif pd.isna(obj):
        return None
    elif isinstance(obj, (np.integer, np.floating)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.DataFrame):
        return {str(col): obj[col].to_dict() for col in obj.columns}
    elif isinstance(obj, pd.Series):
        return obj.to_dict()
    return str(obj)

def get_quarterly_earnings(ticker_symbol):
    """Fetch quarterly earnings data for a given ticker."""
    try:
        ticker = yf.Ticker(ticker_symbol)
        
        # Get the data
        data = {
            'ticker': ticker_symbol,
            'last_updated': datetime.now().isoformat(),
            'data_available': False,
            'quarterly_income_statement': None,
            'error': None
        }
        
        # Add company info if available
        try:
            info = ticker.info
            data['company_name'] = info.get('longName', '')
            data['sector'] = info.get('sector', '')
            data['industry'] = info.get('industry', '')
            data['currency'] = info.get('currency', 'INR')
        except Exception as e:
            data['info_error'] = str(e)
        
        # Get quarterly income statement
        try:
            if hasattr(ticker, 'quarterly_income_stmt') and ticker.quarterly_income_stmt is not None:
                qis = ticker.quarterly_income_stmt
                if not qis.empty:
                    # Convert the DataFrame to a dictionary with proper date formatting
                    qis_dict = {}
                    for idx, row in qis.iterrows():
                        # Convert the row (which is a Series) to a dictionary
                        row_dict = row.to_dict()
                        # Convert any datetime objects to strings
                        row_dict = {str(k): v for k, v in row_dict.items()}
                        qis_dict[str(idx)] = row_dict
                    
                    data['quarterly_income_statement'] = qis_dict
                    data['data_available'] = True
        except Exception as e:
            data['error'] = f"Error fetching quarterly income statement: {str(e)}"
            # Add traceback for better error diagnosis
            import traceback
            data['traceback'] = traceback.format_exc()
        
        return data
        
    except Exception as e:
        return {
            'ticker': ticker_symbol,
            'last_updated': datetime.now().isoformat(),
            'data_available': False,
            'error': str(e)
        }

def main():
    # Load tickers
    with open(TICKERS_FILE, 'r') as f:
        tickers = [line.strip() for line in f if line.strip()]
    
    print(f"Loaded {len(tickers)} tickers")
    
    # Process each ticker
    for ticker in tqdm(tickers, desc="Downloading quarterly earnings"):
        ticker_clean = ticker.replace('.NS', '')
        output_file = os.path.join(OUTPUT_DIR, f'{ticker_clean}.json')
        
        # Skip if file was updated today
        if os.path.exists(output_file):
            file_mtime = datetime.fromtimestamp(os.path.getmtime(output_file)).date()
            if file_mtime == datetime.now().date():
                continue
        
        # Get data with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                data = get_quarterly_earnings(ticker)
                
                # Save to file
                with open(output_file, 'w') as f:
                    json.dump(data, f, default=safe_serialize, indent=2)
                break
                    
            except Exception as e:
                if attempt == max_retries - 1:  # Last attempt
                    print(f"\nError processing {ticker}: {str(e)}")
                time.sleep(2 ** attempt)  # Exponential backoff
        
        # Be nice to the API
        time.sleep(1)
    
    print("\nQuarterly earnings download complete!")

if __name__ == "__main__":
    import pandas as pd
    import numpy as np
    main()
