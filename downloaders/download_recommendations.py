import os
import json
import time
import yfinance as yf
from datetime import datetime
import pandas as pd
import numpy as np
from tqdm import tqdm

# Configuration
OUTPUT_DIR = 'data/recommendations'
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

def get_recommendations_data(ticker_symbol):
    """Fetch recommendations data for a given ticker."""
    try:
        ticker = yf.Ticker(ticker_symbol)
        
        # Initialize data dictionary
        data = {
            'ticker': ticker_symbol,
            'last_updated': datetime.now().isoformat(),
            'data_available': False,
            'recommendations': None,
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
        
        # Get recommendations data
        try:
            recs = ticker.recommendations
            if recs is not None and not recs.empty:
                # Convert the DataFrame to a list of dictionaries
                recommendations = []
                for idx, row in recs.iterrows():
                    rec_dict = row.to_dict()
                    # Convert any non-serializable objects
                    rec_dict = {k: safe_serialize(v) for k, v in rec_dict.items()}
                    rec_dict['date'] = idx.isoformat() if hasattr(idx, 'isoformat') else str(idx)
                    recommendations.append(rec_dict)
                
                if recommendations:
                    data['recommendations'] = recommendations
                    data['data_available'] = True
                    
                    # Add summary statistics
                    if len(recommendations) > 0:
                        # Get the most recent recommendation
                        latest = recommendations[0]
                        data['latest_recommendation'] = {
                            'date': latest.get('date'),
                            'firm': latest.get('firm'),
                            'to_grade': latest.get('toGrade'),
                            'action': latest.get('action')
                        }
                        
        except Exception as e:
            data['error'] = f"Error fetching recommendations: {str(e)}"
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
    for ticker in tqdm(tickers, desc="Downloading recommendations"):
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
                data = get_recommendations_data(ticker)
                
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
    
    print("\nRecommendations data download complete!")

if __name__ == "__main__":
    main()
