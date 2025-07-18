"""
Download income statement data for NSE stocks.

This script downloads both annual and quarterly income statements.
"""
import os
import json
import yfinance as yf
from tqdm import tqdm
import time
from datetime import datetime
import logging
import argparse
from pathlib import Path

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

def get_income_data(ticker_symbol: str, period: str = 'annual', max_retries: int = 3) -> dict:
    """Fetch income statement data for a given ticker and period.
    
    Args:
        ticker_symbol: Stock ticker symbol (with or without .NS)
        period: 'annual' or 'quarterly'
        max_retries: Maximum number of retry attempts
        
    Returns:
        dict: Formatted income statement data
    """
    ticker_clean = ticker_symbol.upper().replace('.NS', '').strip()
    if not ticker_clean:
        return None
    
    ticker = yf.Ticker(f"{ticker_clean}.NS")
    
    for attempt in range(max_retries):
        try:
            # Get income statement
            if period == 'annual':
                stmt = ticker.financials
            else:  # quarterly
                stmt = ticker.quarterly_financials
                
            if stmt is None or stmt.empty:
                return None
                
            # Convert to list of dicts (one per period)
            periods = []
            for col in stmt.columns:
                period_data = {
                    'period_ending': col.strftime('%Y-%m-%d'),
                    'total_revenue': stmt[col].get('Total Revenue'),
                    'operating_income': stmt[col].get('Operating Income'),
                    'net_income': stmt[col].get('Net Income'),
                    'basic_eps': stmt[col].get('Basic EPS'),
                    'diluted_eps': stmt[col].get('Diluted EPS')
                }
                # Only include if we have valid data
                if any(v is not None for v in period_data.values()):
                    periods.append(period_data)
            
            return {
                'ticker': ticker_clean,
                'last_updated': datetime.now().isoformat(),
                'period': period,
                'periods': periods,
                'data_available': bool(periods)
            }
            
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to fetch {period} income data for {ticker_clean}: {str(e)}")
                return None
            time.sleep(2 ** attempt)  # Exponential backoff
    
    return None

def save_data(data: dict, output_dir: str, period: str):
    """Save data to JSON file."""
    if not data or not data.get('data_available'):
        return False
        
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{data['ticker']}.json")
    
    try:
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        logger.error(f"Error saving data for {data['ticker']}: {str(e)}")
        return False

def process_tickers(tickers: list, output_dir: str, period: str = 'annual', force: bool = False):
    """Process list of tickers and download income statements."""
    os.makedirs(output_dir, exist_ok=True)
    processed = 0
    skipped = 0
    failed = []
    
    with tqdm(total=len(tickers), desc=f"Downloading {period} income statements") as pbar:
        for ticker in tickers:
            ticker_clean = ticker.replace('.NS', '')
            output_file = os.path.join(output_dir, f"{ticker_clean}.json")
            
            # Skip if file exists and we're not forcing
            if not force and os.path.exists(output_file):
                skipped += 1
                pbar.update(1)
                continue
                
            # Get data
            data = get_income_data(ticker, period)
            
            # Save if we got valid data
            if data and data.get('data_available'):
                if save_data(data, output_dir, period):
                    processed += 1
                else:
                    failed.append(ticker_clean)
            else:
                failed.append(ticker_clean)
                
            pbar.update(1)
            time.sleep(1)  # Rate limiting
    
    return {
        'total': len(tickers),
        'processed': processed,
        'skipped': skipped,
        'failed': len(failed),
        'failed_tickers': failed
    }

def main():
    parser = argparse.ArgumentParser(description='Download income statement data for NSE stocks')
    parser.add_argument('--period', choices=['annual', 'quarterly'], default='annual',
                      help='Period for income statements (annual or quarterly)')
    parser.add_argument('--force', action='store_true',
                      help='Force download even if file exists')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # Set up paths
    base_dir = Path(__file__).parent.parent
    ticker_file = base_dir / 'tickers_master.txt'
    output_dir = base_dir / 'data' / f"{args.period}_income_statements"
    
    logger.info(f"Starting {args.period} income statement download...")
    
    # Load tickers
    try:
        tickers = load_tickers(ticker_file)
    except Exception as e:
        logger.error(f"Failed to load tickers: {str(e)}")
        return 1
    
    # Process tickers
    start_time = time.time()
    result = process_tickers(tickers, str(output_dir), args.period, args.force)
    
    # Print summary
    duration = time.time() - start_time
    logger.info(f"\n{'='*50}")
    logger.info(f"COMPLETED IN {duration:.2f} SECONDS")
    logger.info(f"Total tickers: {result['total']}")
    logger.info(f"Successfully processed: {result['processed']}")
    logger.info(f"Skipped (already exists): {result['skipped']}")
    logger.info(f"Failed: {result['failed']}")
    
    if result['failed_tickers']:
        logger.info("\nFailed tickers:" + "\n".join(result['failed_tickers']))
    
    return 0

if __name__ == "__main__":
    exit(main())
