#!/usr/bin/env python3
"""
Load balance sheet data from JSON files into database
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
    'total_revenue': ['Total Revenue'],
    'operating_income': ['Operating Income'],
    'net_income': ['Net Income'],
    'basic_eps': ['Basic EPS'],
    'diluted_eps': ['Diluted EPS']
}

def load_income_statement_data_for_ticker(ticker, data_dir):
    """Load income statement data from JSON for a single ticker"""
    # Remove .NS suffix for filename
    ticker_clean = ticker.replace('.NS', '')
    json_file = data_dir / f"{ticker_clean}.json"
    
    if not json_file.exists():
        return None
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        annual_data = data.get('income_statement', {})
        if not annual_data:
            return None
        
        # Get all dates and filter for March 31st only
        all_dates = set()
        for field_data in annual_data.values():
            if isinstance(field_data, dict):
                all_dates.update(field_data.keys())
        
        march_dates = []
        for date_str in all_dates:
            try:
                date_part = date_str.split(' ')[0] if ' 00:00:00' in date_str else date_str
                parsed_date = datetime.strptime(date_part, '%Y-%m-%d').date()
                if parsed_date.month == 3 and parsed_date.day == 31:
                    march_dates.append((date_str, parsed_date))
            except ValueError:
                continue
        
        if not march_dates:
            return None
        
        # Extract records for each March 31st date
        records = []
        for date_str, period_ending in march_dates:
            record = {
                'ticker': ticker,
                'period_ending': period_ending,
                'total_revenue': None,
                'operating_income': None,
                'net_income': None,
                'basic_eps': None,
                'diluted_eps': None
            }
            
            # Extract financial metrics - only add valid values
            for db_field, json_fields in FIELD_MAPPINGS.items():
                for field_name in json_fields:
                    if field_name in annual_data:
                        field_data = annual_data[field_name]
                        if isinstance(field_data, dict) and date_str in field_data:
                            raw_value = field_data[date_str]
                            # Strict validation - reject any NaN, None, or invalid values
                            if (raw_value is not None and 
                                not pd.isna(raw_value) and 
                                str(raw_value).lower() not in ['nan', 'null', 'none', '']):
                                try:
                                    val = float(raw_value)
                                    if math.isfinite(val) and abs(val) < 1e15 and val != 0:
                                        record[db_field] = val
                                        break
                                except (ValueError, TypeError):
                                    continue
            
            # Only keep records with at least 3 valid financial metrics
            non_null_count = sum(1 for k, v in record.items() 
                               if k not in ['ticker', 'period_ending'] and v is not None)
            if non_null_count >= 3:
                records.append(record)
        
        if not records:
            return None
            
        # Convert to DataFrame following price history pattern
        df = pd.DataFrame(records)
        df['ticker'] = ticker  # Ensure ticker is set consistently
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
    
    data_dir = Path(__file__).parent.parent / "data" / "income_statements"
    
    print(f"Processing {len(tickers)} tickers...")
    
    processed = 0
    inserted_total = 0
    
    for ticker in tqdm(tickers, desc="Loading income statement data"):
        df = load_income_statement_data_for_ticker(ticker, data_dir)
        
        if df is None or df.empty:
            continue
        
        # Insert data
        try:
            cur = conn.cursor()
            data = []
            for _, row in df.iterrows():
                # Final validation - replace None with 0 and validate all values
                validated_row = []
                for field in ['ticker', 'period_ending', 'total_revenue', 'operating_income', 'net_income', 'basic_eps', 'diluted_eps']:
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
                INSERT INTO income_statement_annual (
                    ticker, period_ending, total_revenue, operating_income, 
                    net_income, basic_eps, diluted_eps, last_updated
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (ticker, period_ending) DO UPDATE SET
                    total_revenue = EXCLUDED.total_revenue,
                    operating_income = EXCLUDED.operating_income,
                    net_income = EXCLUDED.net_income,
                    basic_eps = EXCLUDED.basic_eps,
                    diluted_eps = EXCLUDED.diluted_eps,
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
