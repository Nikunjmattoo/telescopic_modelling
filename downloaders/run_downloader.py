"""
Main script to run financial data downloads.

Usage:
    python run_downloader.py [data_type] [--period annual|quarterly] [--force]
    
Available data types:
    - income_statement
    - balance_sheet
    - cash_flow
    - price_history
    - company_info
    - all
"""
import argparse
import importlib
import sys
from pathlib import Path
import logging
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

AVAILABLE_DOWNLOADERS = {
    'income_statement': ['annual', 'quarterly'],
    'balance_sheet': ['annual', 'quarterly'],
    'cash_flow': ['annual', 'quarterly'],
    'price_history': [''],
    'company_info': [''],
    'all': ['']
}

def run_downloader(downloader: str, period: str = '', force: bool = False) -> bool:
    """Run a specific downloader module."""
    try:
        if downloader == 'all':
            success = True
            for dl, periods in AVAILABLE_DOWNLOADERS.items():
                if dl == 'all':
                    continue
                for p in periods:
                    if not run_downloader(dl, p, force):
                        success = False
            return success
            
        module_name = f"downloaders.{downloader}"
        if period:
            module_name += f"_{period}"
            
        try:
            module = importlib.import_module(module_name.replace('/', '.'))
        except ModuleNotFoundError:
            logger.error(f"No downloader found for {downloader} {period}")
            return False
            
        logger.info(f"Running {downloader} {period}...")
        return module.main(force=force)
        
    except Exception as e:
        logger.error(f"Error running {downloader}: {str(e)}", exc_info=True)
        return False

def main():
    parser = argparse.ArgumentParser(description='Download financial data')
    parser.add_argument('data_type', nargs='?', default='all',
                      choices=list(AVAILABLE_DOWNLOADERS.keys()) + ['all'],
                      help='Type of data to download')
    parser.add_argument('--period', choices=['annual', 'quarterly'],
                      help='Period for financial data (annual/quarterly)')
    parser.add_argument('--force', action='store_true',
                      help='Force download even if file exists')
    parser.add_argument('--debug', action='store_true',
                      help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Validate period
    if args.period and args.period not in AVAILABLE_DOWNLOADERS.get(args.data_type, ['']):
        logger.error(f"Period '{args.period}' not supported for {args.data_type}")
        return 1
    
    # Run the downloader
    success = run_downloader(args.data_type, args.period or '', args.force)
    
    if success:
        logger.info("Download completed successfully")
        return 0
    else:
        logger.error("Download completed with errors")
        return 1

if __name__ == "__main__":
    sys.exit(main())
