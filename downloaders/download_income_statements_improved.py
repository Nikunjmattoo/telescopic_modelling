import os
import json
import yfinance as yf
from tqdm import tqdm
import time
from datetime import datetime
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download_income_statements.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_tickers(ticker_file):
    """Load tickers from the master file."""
    try:
        with open(ticker_file, 'r') as f:
            tickers = [line.strip() for line in f if line.strip()]
        logger.info(f"Loaded {len(tickers)} tickers from {ticker_file}")
        return tickers
    except Exception as e:
        logger.error(f"Error loading tickers from {ticker_file}: {str(e)}")
        raise

def convert_timestamps(obj):
    """Recursively convert Timestamp objects to strings."""
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d')
    elif isinstance(obj, dict):
        return {str(k): convert_timestamps(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_timestamps(x) for x in obj]
    return obj

def get_earnings_data(ticker_symbol, max_retries=3, initial_delay=1):
    """Fetch earnings data for a given ticker with robust error handling."""
    try:
        ticker_clean = ticker_symbol.upper().replace('.NS', '').strip()
        if not ticker_clean:
            logger.warning(f"Invalid ticker symbol: {ticker_symbol}")
            return None
            
        ticker = yf.Ticker(f"{ticker_clean}.NS")
        
        # Initialize data structure with metadata
        earnings_data = {
            'ticker': ticker_clean,
            'last_updated': datetime.now().isoformat(),
            'currency': 'INR',
            'data_available': False
        }
        
        for attempt in range(max_retries):
            try:
                # Add delay with exponential backoff
                if attempt > 0:
                    delay = initial_delay * (2 ** (attempt - 1))
                    logger.info(f"Retrying {ticker_clean} in {delay} seconds (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                
                # Get basic info
                info = ticker.info
                earnings_data['company_name'] = info.get('longName', '')
                earnings_data['sector'] = info.get('sector', '')
                earnings_data['industry'] = info.get('industry', '')
                earnings_data['currency'] = info.get('currency', 'INR')
                
                # Get income statement
                income_stmt = ticker.income_stmt
                if income_stmt is not None and not income_stmt.empty:
                    earnings_data['income_statement'] = convert_timestamps(income_stmt.to_dict())
                    earnings_data['data_available'] = True
                    
                    # Add additional data if available
                    try:
                        balance_sheet = ticker.balance_sheet
                        if balance_sheet is not None and not balance_sheet.empty:
                            earnings_data['balance_sheet'] = convert_timestamps(balance_sheet.to_dict())
                    except Exception as e:
                        logger.warning(f"Could not fetch balance sheet for {ticker_clean}: {str(e)}")
                    
                    try:
                        cash_flow = ticker.cashflow
                        if cash_flow is not None and not cash_flow.empty:
                            earnings_data['cash_flow'] = convert_timestamps(cash_flow.to_dict())
                    except Exception as e:
                        logger.warning(f"Could not fetch cash flow for {ticker_clean}: {str(e)}")
                    
                    return earnings_data
                
            except Exception as e:
                if '404' in str(e):
                    logger.warning(f"Data not found for {ticker_clean} (404)")
                    return None
                logger.warning(f"Attempt {attempt + 1} failed for {ticker_clean}: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch data for {ticker_clean} after {max_retries} attempts")
                continue
                
        return None
        
    except Exception as e:
        logger.error(f"Unexpected error processing {ticker_symbol}: {str(e)}", exc_info=True)
        return None

def main(force_download=False):
    try:
        # Create output directory
        output_dir = 'data/income_statements'
        os.makedirs(output_dir, exist_ok=True)
        
        # Load tickers
        tickers = load_tickers('c:/Projects/equity_allocator/tickers_master.txt')
        
        # Track progress
        processed = set()
        failed_tickers = []
        
        # Get list of existing files
        existing_files = set()
        if os.path.exists(output_dir):
            existing_files = {f[:-5] for f in os.listdir(output_dir) if f.endswith('.json')}
        
        # Process each ticker
        with tqdm(total=len(tickers), desc="Downloading income statements") as pbar:
            for i, ticker in enumerate(tickers, 1):
                # Clean the ticker (remove .NS if present)
                ticker_clean = ticker.replace('.NS', '')
                output_file = os.path.join(output_dir, f'{ticker_clean}.json')
                
                # Skip if file exists and we're not forcing download
                if os.path.exists(output_file) and not force_download:
                    file_time = os.path.getmtime(output_file)
                    file_date = datetime.fromtimestamp(file_time).date()
                    today = datetime.now().date()
                    
                    if file_date == today:
                        logger.debug(f"Skipping {ticker_clean} - already downloaded today")
                        pbar.update(1)
                        continue
                    else:
                        logger.debug(f"File for {ticker_clean} exists but is from {file_date}, will be updated")
                
                logger.info(f"Processing ticker {i}/{len(tickers)}: {ticker_clean}")
                time.sleep(1)  # Add a small delay to avoid rate limiting
                output_file = os.path.join(output_dir, f'{ticker_clean}.json')
                
                # Skip if already processed in this session
                if ticker_clean in processed:
                    pbar.update(1)
                    continue
                    
                # Check if file exists and handle skipping
                if os.path.exists(output_file):
                    file_time = os.path.getmtime(output_file)
                    file_date = datetime.fromtimestamp(file_time).date()
                    today = datetime.now().date()
                    
                    if not force_download:
                        if file_date == today:
                            logger.debug(f"Skipping {ticker_clean} - already downloaded today")
                            processed.add(ticker_clean)
                            pbar.update(1)
                            continue
                        else:
                            logger.debug(f"File for {ticker_clean} exists but is from {file_date}, will be updated")
                    else:
                        logger.debug(f"Force mode: Re-downloading {ticker_clean}")
                else:
                    logger.debug(f"No existing file found for {ticker_clean}, will download")
                
                try:
                    # Get earnings data
                    data = get_earnings_data(ticker_clean)
                    
                    # Save to file if data is available
                    if data and data.get('data_available'):
                        with open(output_file, 'w') as f:
                            json.dump(data, f, indent=2)
                        logger.info(f"Successfully processed {ticker_clean} ({i}/{len(tickers)})")
                    else:
                        logger.warning(f"No data available for {ticker_clean}")
                        failed_tickers.append(ticker_clean)
                    
                    # Add delay to be nice to the API
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Error processing {ticker_clean}: {str(e)}", exc_info=True)
                    failed_tickers.append(ticker_clean)
                
                processed.add(ticker_clean)
                pbar.update(1)
        
        # Log completion
        success_count = len(processed) - len(failed_tickers)
        logger.info(f"\nProcessing complete!")
        logger.info(f"Successfully processed: {success_count}/{len(tickers)} tickers")
        logger.info(f"Failed to process: {len(failed_tickers)} tickers")
        
        if failed_tickers:
            logger.info("\nFailed tickers:" + "\n".join(failed_tickers))
        
    except Exception as e:
        logger.critical(f"Fatal error in main: {str(e)}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    import argparse
    
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Download income statements data for NSE stocks.')
    parser.add_argument('--force', action='store_true', 
                       help='Force download even if file exists')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    args = parser.parse_args()
    
    # Set log level
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    
    start_time = time.time()
    logger.info("Starting income statements download...")
    if args.force:
        logger.warning("Force download enabled - existing files will be overwritten")
    
    exit_code = main(force_download=args.force)
    
    duration = time.time() - start_time
    logger.info(f"Script completed in {duration:.2f} seconds")
    
    exit(exit_code)
