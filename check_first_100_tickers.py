#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Check the first 100 tickers from nse_tickers.csv to identify equity tickers
"""
import sys
import time
import yfinance as yf
from pathlib import Path
from tqdm import tqdm

# Set console output encoding to UTF-8
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def load_tickers(file_path, limit=100):
    """Load tickers from file"""
    try:
        with open(file_path, 'r') as f:
            return [line.strip() for line in f if line.strip()][:limit]
    except Exception as e:
        print(f"[ERROR] Failed to load tickers: {e}")
        sys.exit(1)

def check_ticker(ticker):
    """Check if a ticker is valid and get its type"""
    try:
        # Ensure ticker has .NS suffix
        if not ticker.endswith('.NS'):
            ticker = f"{ticker}.NS"
        
        # Get ticker info
        ticker_obj = yf.Ticker(ticker)
        info = ticker_obj.info
        
        # Get ticker type
        ticker_type = info.get('quoteType', info.get('type', 'unknown'))
        
        return {
            'ticker': ticker,
            'type': ticker_type,
            'name': info.get('shortName', info.get('longName', 'N/A')),
            'sector': info.get('sector', 'N/A'),
            'industry': info.get('industry', 'N/A'),
            'is_equity': ticker_type.lower() == 'equity'
        }
        
    except Exception as e:
        return {
            'ticker': ticker,
            'type': 'error',
            'error': str(e),
            'is_equity': False
        }

def main():
    # File paths
    input_file = Path("nse_tickers.csv")
    
    if not input_file.exists():
        print(f"[ERROR] File not found: {input_file}")
        sys.exit(1)
    
    # Load first 100 tickers
    print(f"Loading first 100 tickers from {input_file}...")
    tickers = load_tickers(input_file, 100)
    
    if not tickers:
        print("[ERROR] No tickers found in the file")
        sys.exit(1)
    
    print(f"Found {len(tickers)} tickers to check")
    print("-" * 80)
    
    # Check each ticker
    results = []
    for ticker in tqdm(tickers, desc="Checking tickers"):
        result = check_ticker(ticker)
        results.append(result)
        
        # Print summary of the last check
        if 'error' in result:
            print(f"{ticker}: Error - {result['error']}")
        else:
            print(f"{ticker}: {result['type']} - {result['name']}")
        
        # Be nice to the API
        time.sleep(1)
    
    # Print summary
    print("\n" + "="*80)
    print("CHECK SUMMARY")
    print("="*80)
    
    # Count by type
    types = {}
    for r in results:
        t = r.get('type', 'unknown')
        types[t] = types.get(t, 0) + 1
    
    # Print type distribution
    print("\nTYPES FOUND:")
    for t, count in sorted(types.items()):
        print(f"- {t}: {count}")
    
    # Print equity tickers
    equity_tickers = [r for r in results if r.get('is_equity')]
    print(f"\nFOUND {len(equity_tickers)} EQUITY TICKERS:")
    for r in equity_tickers:
        print(f"- {r['ticker']}: {r['name']} ({r['sector']} - {r['industry']})")
    
    # Print errors
    error_tickers = [r for r in results if 'error' in r]
    if error_tickers:
        print(f"\n{len(error_tickers)} TICKERS HAD ERRORS:")
        for r in error_tickers[:10]:  # Print first 10 errors
            print(f"- {r['ticker']}: {r['error']}")
        if len(error_tickers) > 10:
            print(f"... and {len(error_tickers) - 10} more")
    
    print("\n" + "="*80)
    print(f"Checked {len(tickers)} tickers, found {len(equity_tickers)} equity tickers")
    print("="*80)

if __name__ == "__main__":
    main()
