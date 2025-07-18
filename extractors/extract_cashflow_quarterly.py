#!/usr/bin/env python3
"""
Load quarterly cash flow data from JSON files into database
Following the same pattern as extract_price_history.py
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

# Field mappings based on JSON analysis
FIELD_MAPPINGS = {
    'operating_cash_flow': ['Operating Cash Flow', 'Cash Flow from Operating Activities'],
    'free_cash_flow': ['Free Cash Flow', 'Free Cash Flow to Firm'],
    'dividends_paid': ['Cash Dividends Paid', 'Common Stock Dividend Paid']
}

def load_quarterly_cash_flow_data_for_ticker(ticker, data_dir):
    """Load quarterly cash flow data from JSON for a single ticker"""
    # Remove .NS suffix for filename lookup
    ticker_clean = ticker.replace('.NS', '').upper()
    json_file = data_dir / f"{ticker_clean}.json"
    
    if not json_file.exists():
        return None
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        quarterly_data = data.get('quarterly_cashflow', {})
        if not quarterly_data:
            return None
        
        # Get all dates and filter for quarter-end dates (Mar 31, Jun 30, Sep 30, Dec 31)
        all_dates = set()
        for field_data in quarterly_data.values():
            if isinstance(field_data, dict):
                all_dates.update(field_data.keys())
        
        quarter_end_dates = []
        for date_str in all_dates:
            try:
                date_part = date_str.split(' ')[0] if ' 00:00:00' in date_str else date_str
                parsed_date = datetime.strptime(date_part, '%Y-%m-%d').date()
                # Check for quarter-end dates
                if ((parsed_date.month == 3 and parsed_date.day == 31) or
                    (parsed_date.month == 6 and parsed_date.day == 30) or
                    (parsed_date.month == 9 and parsed_date.day == 30) or
                    (parsed_date.month == 12 and parsed_date.day == 31)):
                    quarter_end_dates.append((date_str, parsed_date))
            except ValueError:
                continue
        
        if not quarter_end_dates:
            return None
        
        # Extract records for each quarter-end date
        records = []
        for date_str, period_ending in quarter_end_dates:
            record = {
                'ticker': original_ticker,  # Use the original ticker with .NS
                'period_ending': period_ending,
                'operating_cash_flow': None,
                'free_cash_flow': None,
                'dividends_paid': None
            }
            
            # Extract financial metrics - only add valid values
            for db_field, json_fields in FIELD_MAPPINGS.items():
                for field_name in json_fields:
                    if field_name in quarterly_data:
                        field_data = quarterly_data[field_name]
                        if isinstance(field_data, dict) and date_str in field_data:
                            raw_value = field_data[date_str]
                            # Validation - handle different field types appropriately
                            if (raw_value is not None and 
                                not pd.isna(raw_value) and 
                                str(raw_value).lower() not in ['nan', 'null', 'none', '']):
                                try:
                                    val = float(raw_value)
                                    # Special handling for dividends (can be 0)
                                    if db_field == 'dividends_paid':
                                        if math.isfinite(val) and abs(val) < 1e15:
                                            record[db_field] = val
                                            break
                                    # For other fields, require non-zero values
                                    elif math.isfinite(val) and abs(val) < 1e15 and val != 0:
                                        record[db_field] = val
                                        break
                                except (ValueError, TypeError):
                                    continue
            
            # Keep all records regardless of null values
            records.append(record)
        
        if not records:
            return None
            
        # Convert to DataFrame following price history pattern
        df = pd.DataFrame(records)
        df['ticker'] = original_ticker  # Ensure ticker is set consistently with .NS
        return df
        
    except Exception as e:
        print(f"Error processing {ticker}: {str(e)}")
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
    data_dir = Path(__file__).parent.parent / "data" / "quarterly_cashflow"
    data_dir.mkdir(parents=True, exist_ok=True)  # Ensure directory exists
    
    # Print debug info
    print("Starting quarterly cash flow data extraction")
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
    
    print(f"Processing {len(tickers)} tickers...")
    
    processed = 0
    inserted_total = 0
    
    for ticker in tqdm(tickers, desc="Loading quarterly cash flow data"):
        df = load_quarterly_cash_flow_data_for_ticker(ticker, data_dir)
        
        if df is None or df.empty:
            continue
        
        # Insert data
        try:
            cur = conn.cursor()
            data = []
            for _, row in df.iterrows():
                # Final validation - replace None with 0 and validate all values
                validated_row = []
                for field in ['ticker', 'period_ending', 'operating_cash_flow', 'free_cash_flow', 'dividends_paid']:
                    value = row[field]
                    if field in ['ticker', 'period_ending']:
                        validated_row.append(value)
                    else:
                        # Convert None/NaN and 0 values to NULL for financial fields
                        if value is None or pd.isna(value) or value == 0.0:
                            validated_row.append(None)  # Will become NULL in database
                        else:
                            validated_row.append(float(value))
                
                data.append(tuple(validated_row))
            
            cur.executemany("""
                INSERT INTO cash_flow_quarterly (
                    ticker, period_ending, operating_cash_flow, free_cash_flow, 
                    dividends_paid, last_updated
                )
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (ticker, period_ending) DO UPDATE SET
                    operating_cash_flow = EXCLUDED.operating_cash_flow,
                    free_cash_flow = EXCLUDED.free_cash_flow,
                    dividends_paid = EXCLUDED.dividends_paid,
                    last_updated = EXCLUDED.last_updated
            """, data)
            
            inserted = cur.rowcount
            conn.commit()
            cur.close()
            
            inserted_total += inserted
            processed += 1
            
        except Exception as e:
            conn.rollback()
            print(f"Error inserting data for {ticker}: {str(e)}")
    
    conn.close()
    
    print(f"\n=== SUMMARY ===")
    print(f"Processed: {processed}/{len(tickers)} tickers")
    print(f"Records inserted: {inserted_total}")

if __name__ == "__main__":
    main()
