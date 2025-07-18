import os
import json
import yfinance as yf
from tqdm import tqdm
import time
from datetime import datetime
import logging
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download_options.log'),
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

def get_options_data(ticker_symbol, max_retries=3, initial_delay=1):
    """Fetch options data for a given ticker with retry logic."""
    try:
        ticker_clean = ticker_symbol.upper().replace('.NS', '').strip()
        if not ticker_clean:
            logger.warning(f"Invalid ticker symbol: {ticker_symbol}")
            return None
            
        ticker = yf.Ticker(f"{ticker_clean}.NS")
        
        for attempt in range(max_retries):
            try:
                options = ticker.options
                if options and len(options) > 0:
                    return {
                        'ticker': ticker_clean,
                        'expiry_dates': options,
                        'timestamp': datetime.now().isoformat(),
                        'data_available': True
                    }
                else:
                    logger.warning(f"No options data available for {ticker_clean}")
                    return {
                        'ticker': ticker_clean,
                        'expiry_dates': [],
                        'timestamp': datetime.now().isoformat(),
                        'data_available': False
                    }
                    
            except Exception as e:
                if "404" in str(e):
                    logger.warning(f"Options data not found for {ticker_clean} (404)")
                    return {
                        'ticker': ticker_clean,
                        'expiry_dates': [],
                        'timestamp': datetime.now().isoformat(),
                        'data_available': False
                    }
                
                if attempt < max_retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    logger.info(f"Retrying {ticker_clean} in {delay} seconds (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                else:
                    logger.error(f"Failed to fetch options for {ticker_clean} after {max_retries} attempts")
        
        return None
        
    except Exception as e:
        logger.error(f"Unexpected error processing {ticker_symbol}: {str(e)}", exc_info=True)
        return None

def main(force_download=False):
    try:
        # Create output directory
        output_dir = 'data/options'
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
        with tqdm(total=len(tickers), desc="Downloading options data") as pbar:
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
                
                try:
                    # Get options data
                    data = get_options_data(ticker_clean)
                    
                    # Save to file if data is available
                    if data:
                        with open(output_file, 'w') as f:
                            json.dump(data, f, indent=2)
                        logger.info(f"Successfully processed {ticker_clean} ({i}/{len(tickers)})")
                    else:
                        logger.warning(f"No options data available for {ticker_clean}")
                        failed_tickers.append(ticker_clean)
                    
                except Exception as e:
                    logger.error(f"Error processing {ticker_clean}: {str(e)}", exc_info=True)
                    failed_tickers.append(ticker_clean)
                
                pbar.update(1)
        
        # Print summary
        logger.info("\nProcessing complete!")
        logger.info(f"Successfully processed: {len(tickers) - len(failed_tickers)}/{len(tickers)} tickers")
        logger.info(f"Failed to process: {len(failed_tickers)} tickers")
        
        if failed_tickers:
            logger.info("\nFailed tickers:" + "\n".join(failed_tickers))
        
        return 0
        
    except Exception as e:
        logger.error(f"Script failed: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    import argparse
    
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Download options data for NSE stocks.')
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
    logger.info("Starting options data download...")
    if args.force:
        logger.warning("Force download enabled - existing files will be overwritten")
    
    exit_code = main(force_download=args.force)
    
    duration = time.time() - start_time
    logger.info(f"Script completed in {duration:.2f} seconds")
    
    exit(exit_code)
