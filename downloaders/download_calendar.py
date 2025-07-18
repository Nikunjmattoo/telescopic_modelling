import os
import json
import yfinance as yf
from tqdm import tqdm
import time
from datetime import datetime
import pandas as pd
import logging
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download_calendar.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_tickers(ticker_file: str) -> list:
    """Load tickers from the master file."""
    try:
        with open(ticker_file, 'r') as f:
            tickers = [line.strip() for line in f if line.strip()]
        logger.info(f"Loaded {len(tickers)} tickers from {ticker_file}")
        return tickers
    except Exception as e:
        logger.error(f"Error loading tickers from {ticker_file}: {str(e)}")
        raise

def safe_serialize(obj: Any) -> Any:
    """Convert non-serializable objects to strings."""
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    elif pd.isna(obj):
        return None
    elif isinstance(obj, (pd.DataFrame, pd.Series)):
        return obj.to_dict()
    elif isinstance(obj, (int, float, str, bool)) or obj is None:
        return obj
    return str(obj)

def get_calendar_data(ticker_symbol: str, max_retries: int = 3, initial_delay: int = 1) -> Optional[Dict]:
    """Fetch calendar data for a given ticker with retry logic."""
    try:
        ticker_clean = ticker_symbol.upper().replace('.NS', '').strip()
        if not ticker_clean:
            logger.warning(f"Invalid ticker symbol: {ticker_symbol}")
            return None
            
        ticker = yf.Ticker(f"{ticker_clean}.NS")
        
        # Initialize data structure with metadata
        calendar_data = {
            'ticker': ticker_clean,
            'last_updated': datetime.now().isoformat(),
            'data_available': False,
            'calendar': None,
            'error': None
        }
        
        for attempt in range(max_retries):
            try:
                # Add delay with exponential backoff
                if attempt > 0:
                    delay = initial_delay * (2 ** (attempt - 1))
                    logger.info(f"Retrying {ticker_clean} in {delay} seconds (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                
                # Get basic info with error handling
                try:
                    info = ticker.info
                    calendar_data.update({
                        'company_name': info.get('longName', ''),
                        'sector': info.get('sector', ''),
                        'industry': info.get('industry', ''),
                        'currency': info.get('currency', 'INR')
                    })
                except Exception as e:
                    logger.debug(f"Could not fetch info for {ticker_clean}: {str(e)}")
                
                # Get calendar data with robust error handling
                try:
                    calendar = ticker.calendar
                    if calendar is not None:
                        if hasattr(calendar, 'empty') and not calendar.empty:
                            # Handle pandas DataFrame case
                            calendar_data['calendar'] = {
                                'earnings_date': safe_serialize(calendar.get('Earnings Date')),
                                'earnings_average': safe_serialize(calendar.get('Earnings Average')),
                                'earnings_low': safe_serialize(calendar.get('Earnings Low')),
                                'earnings_high': safe_serialize(calendar.get('Earnings High')),
                                'revenue_average': safe_serialize(calendar.get('Revenue Average')),
                                'revenue_low': safe_serialize(calendar.get('Revenue Low')),
                                'revenue_high': safe_serialize(calendar.get('Revenue High'))
                            }
                            calendar_data['data_available'] = True
                        elif isinstance(calendar, dict) and calendar:
                            # Handle dictionary case
                            calendar_data['calendar'] = {k: safe_serialize(v) for k, v in calendar.items()}
                            calendar_data['data_available'] = True
                except Exception as e:
                    logger.debug(f"Could not fetch calendar for {ticker_clean}: {str(e)}")
                    # If calendar fetch fails, try to get basic earnings info using a different method
                    try:
                        hist = ticker.history(period='1y')
                        if not hist.empty:
                            calendar_data['data_available'] = True
                            calendar_data['calendar'] = {
                                'last_trade_date': safe_serialize(hist.index[-1] if not hist.empty else None),
                                'last_close': safe_serialize(hist['Close'].iloc[-1] if not hist.empty else None)
                            }
                    except Exception as e2:
                        logger.debug(f"Could not fetch history for {ticker_clean}: {str(e2)}")
                    
                # Get earnings dates if available
                try:
                    earnings_dates = ticker.get_earnings_dates()
                    if earnings_dates is not None and not earnings_dates.empty:
                        calendar_data['earnings_dates'] = safe_serialize(earnings_dates)
                except Exception as e:
                    logger.debug(f"Could not fetch earnings dates for {ticker_clean}: {str(e)}")
                
                return calendar_data
                
            except Exception as e:
                if '404' in str(e):
                    logger.warning(f"Data not found for {ticker_clean} (404)")
                    return None
                logger.warning(f"Attempt {attempt + 1} failed for {ticker_clean}: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch calendar data for {ticker_clean} after {max_retries} attempts")
                    calendar_data['error'] = str(e)
                continue
                
        return calendar_data
        
    except Exception as e:
        logger.error(f"Unexpected error processing {ticker_symbol}: {str(e)}", exc_info=True)
        return {
            'ticker': ticker_symbol,
            'last_updated': datetime.now().isoformat(),
            'data_available': False,
            'error': str(e)
        }

def main(force_download: bool = False):
    try:
        # Create output directory
        output_dir = 'data/calendar'
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
        with tqdm(total=len(tickers), desc="Downloading calendar data") as pbar:
            for i, ticker in enumerate(tickers, 1):
                ticker_clean = ticker.replace('.NS', '')
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
                
                try:
                    # Get calendar data
                    data = get_calendar_data(ticker_clean)
                    
                    # Save to file if data is available
                    if data:
                        with open(output_file, 'w') as f:
                            json.dump(data, f, default=safe_serialize, indent=2)
                        
                        if data.get('data_available'):
                            logger.info(f"Processed {ticker_clean} ({i}/{len(tickers)})")
                        else:
                            logger.warning(f"No calendar data available for {ticker_clean}")
                            failed_tickers.append(ticker_clean)
                    else:
                        logger.warning(f"Failed to process {ticker_clean}")
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
        
        return 0
        
    except Exception as e:
        logger.critical(f"Fatal error in main: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    import argparse
    
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Download calendar data for NSE stocks.')
    parser.add_argument('--force', action='store_true', 
                       help='Force download even if file exists')
    parser.add_argument('--debug', action='store_true',
                       help='Enable debug logging')
    args = parser.parse_args()
    
    # Set log level based on debug flag
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger.setLevel(log_level)
    
    start_time = time.time()
    logger.info("Starting calendar data download...")
    if args.force:
        logger.warning("Force download enabled - existing files will be overwritten")
    
    exit_code = main(force_download=args.force)
    
    duration = time.time() - start_time
    logger.info(f"Script completed in {duration:.2f} seconds")
    
    exit(exit_code)
