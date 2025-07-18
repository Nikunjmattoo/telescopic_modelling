#!/usr/bin/env python3
"""
Load quarterly income statement data from JSON files into database
"""
import json
import pandas as pd
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection
from tqdm import tqdm
from datetime import datetime
import math

# Field mappings for income statement data - using exact field names from JSON
FIELD_MAPPINGS = {
    'total_revenue': ['Total Revenue'],
    'operating_income': ['Operating Income'],
    'net_income': ['Net Income'],
    'basic_eps': ['Earnings Per Share', 'EPS - Basic', 'EPS - Basic (Rs.)'],
    'diluted_eps': ['EPS - Diluted', 'Diluted EPS']
}

def load_quarterly_income_statement_data_for_ticker(ticker, data_dir):
    """Load quarterly income statement data from JSON for a single ticker"""
    # Store original ticker for database (with .NS if present)
    original_ticker = ticker
    # Remove .NS suffix for filename lookup
    ticker_clean = ticker.replace('.NS', '').upper()
    json_file = data_dir / f"{ticker_clean}.json"
    
    if not json_file.exists():
        # Try with .NS suffix if file not found
        if not json_file.name.endswith('.NS.json'):
            json_file = data_dir / f"{ticker_clean}.NS.json"
            if not json_file.exists():
                return None
        else:
            return None
    
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
            
        # Extract ticker data from the JSON structure
        if 'quarterly_income_statement' not in data:
            print(f"No quarterly_income_statement found in {json_file}")
            return None
            
        quarterly_data = data['quarterly_income_statement']
        print(f"Found quarterly_income_statement with {len(quarterly_data)} metrics")
        
        # Get all unique dates from all metrics
        all_dates = set()
        for metric_data in quarterly_data.values():
            if isinstance(metric_data, dict):
                all_dates.update(metric_data.keys())
                
        if not all_dates:
            print(f"No date data found in {json_file}")
            return None
            
        # Convert date strings to datetime objects and filter for quarter-end dates
        quarter_end_dates = []
        for date_str in all_dates:
            try:
                # Extract just the date part before the space
                date_part = date_str.split(' ')[0]
                date_obj = datetime.strptime(date_part, '%Y-%m-%d').date()
                
                # Check if it's a quarter-end date (Mar 31, Jun 30, Sep 30, Dec 31)
                if (date_obj.month in [3, 6, 9, 12] and 
                    ((date_obj.month == 3 and date_obj.day == 31) or
                     (date_obj.month == 6 and date_obj.day == 30) or
                     (date_obj.month == 9 and date_obj.day == 30) or
                     (date_obj.month == 12 and date_obj.day == 31))):
                    quarter_end_dates.append((date_str, date_obj))
            except (ValueError, IndexError):
                continue
                
        if not quarter_end_dates:
            print(f"No quarter-end dates found in {json_file}")
            return None
            
        # Sort by date (newest first)
        quarter_end_dates.sort(key=lambda x: x[1], reverse=True)
        
        # Print sample of the data structure for debugging
        print(f"Sample metrics for {ticker}:")
        for i, (metric, values) in enumerate(quarterly_data.items()):
            if i < 5:  # Show first 5 metrics
                print(f"  {metric}: {list(values.items())[:2]}...")
        
        # Create a copy of quarterly_data to avoid modifying the original
        quarterly_data_copy = quarterly_data.copy()
        records = []
        for date_str, date_obj in quarter_end_dates:
            record = {
                'ticker': original_ticker,
                'period_ending': date_obj,
                'total_revenue': None,
                'operating_income': None,
                'net_income': None,
                'basic_eps': None,
                'diluted_eps': None,
                'last_updated': datetime.now()
            }
            
            # Helper function to safely get a value from the quarterly data
            def get_value(metric_names):
                if not isinstance(metric_names, list):
                    metric_names = [metric_names]
                    
                for metric in metric_names:
                    if (metric in quarterly_data_copy and 
                        isinstance(quarterly_data_copy[metric], dict) and 
                        date_str in quarterly_data_copy[metric] and 
                        quarterly_data_copy[metric][date_str] is not None and 
                        not (isinstance(quarterly_data_copy[metric][date_str], float) and 
                             math.isnan(quarterly_data_copy[metric][date_str]))):
                        return quarterly_data_copy[metric][date_str]
                return None
            
            # Map fields
            record['total_revenue'] = get_value('Total Revenue')
            record['operating_income'] = get_value('Operating Income')
            record['net_income'] = get_value('Net Income')
            record['basic_eps'] = get_value('Basic EPS')
            record['diluted_eps'] = get_value('Diluted EPS')
            
            # Add record if it has any non-null values
            if any(v is not None for k, v in record.items() if k not in ['ticker', 'period_ending', 'last_updated']):
                records.append(record)
        
        if not records:
            print(f"No valid records found for {ticker}")
            return None
            
        print(f"Processed {len(records)} records for {ticker}")
        return pd.DataFrame(records)
        
    except Exception as e:
        print(f"Error processing {ticker}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def main():
    # Connect to database
    db = DatabaseConnection()
    conn = db.connect()
    
    # Get all tickers from database
    cur = conn.cursor()
    cur.execute("SELECT ticker FROM ticker ORDER BY ticker")
    tickers = [row[0] for row in cur.fetchall()]
    cur.close()
    
    # Set up data directory
    data_dir = Path(__file__).parent.parent / "data" / "quarterly_income_statements"
    data_dir.mkdir(parents=True, exist_ok=True)  # Ensure directory exists
    
    # Print debug info
    print("Starting quarterly income statement data extraction")
    print(f"Data directory: {data_dir}")
    print(f"Number of tickers to process: {len(tickers)}")
    print("First 5 tickers:", tickers[:5])
    print("Last 5 tickers:", tickers[-5:])
    print("-" * 80)
    
    # Count existing JSON files for debugging
    json_files = list(data_dir.glob("*.json"))
    print(f"Found {len(json_files)} JSON files in the data directory")
    if json_files:
        print(f"Sample JSON files: {[f.stem for f in json_files[:5]]}...")
    print("-" * 80)
    
    # Check first 5 tickers if their files exist
    print("\nChecking file existence for first 5 tickers:")
    for t in tickers[:5]:
        ticker_clean = t.replace('.NS', '').upper()
        file1 = data_dir / f"{ticker_clean}.json"
        file2 = data_dir / f"{ticker_clean}.NS.json"
        print(f"{t}: {file1.name} exists: {file1.exists()}, {file2.name} exists: {file2.exists()}")
    
    print("\nProcessing tickers...")
    
    processed = 0
    records_inserted = 0
    
    # Process each ticker with progress bar
    for ticker in tqdm(tickers, desc="Loading quarterly income statement data"):
        try:
            df = load_quarterly_income_statement_data_for_ticker(ticker, data_dir)
            
            # Insert data into database
            if df is not None and not df.empty:
                with conn.cursor() as cur:
                    # Convert DataFrame to list of tuples for executemany
                    records = df[['ticker', 'period_ending', 'total_revenue', 
                                'operating_income', 'net_income', 'basic_eps',
                                'diluted_eps', 'last_updated']].to_records(index=False).tolist()
                    
                    # Prepare data for insertion
                    records = df[['ticker', 'period_ending', 'total_revenue', 'operating_income',
                                'net_income', 'basic_eps', 'diluted_eps', 'last_updated']].values.tolist()
                    
                    # Use executemany for batch insert
                    cur.executemany("""
                        INSERT INTO income_statement_quarterly (
                            ticker, period_ending, total_revenue, operating_income,
                            net_income, basic_eps, diluted_eps, last_updated
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::timestamp)
                        ON CONFLICT (ticker, period_ending) 
                        DO UPDATE SET
                            total_revenue = EXCLUDED.total_revenue,
                            operating_income = EXCLUDED.operating_income,
                            net_income = EXCLUDED.net_income,
                            basic_eps = EXCLUDED.basic_eps,
                            diluted_eps = EXCLUDED.diluted_eps,
                            last_updated = EXCLUDED.last_updated
                    """, records)
                    
                    # Get the number of affected rows
                    records_affected = cur.rowcount
                    
                    conn.commit()
                    processed += 1
                    records_inserted += records_affected
                    
        except Exception as e:
            print(f"Error processing {ticker}: {str(e)}")
            conn.rollback()
    
    print("\n=== SUMMARY ===")
    print(f"Processed: {processed}/{len(tickers)} tickers")
    print(f"Records inserted/updated: {records_inserted}")
    
    # Close database connection
    conn.close()

if __name__ == "__main__":
    main()
