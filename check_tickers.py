#!/usr/bin/env python3
"""Check ticker formats in the database"""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent))
from db_utils import DatabaseConnection

def check_ticker_formats():
    db = DatabaseConnection()
    conn = db.connect()
    
    # Get a sample of tickers
    cur = conn.cursor()
    
    # Count total tickers
    cur.execute("SELECT COUNT(*) FROM ticker")
    total_tickers = cur.fetchone()[0]
    
    # Get a sample of tickers with and without .NS
    cur.execute("""
        SELECT ticker, 
               ticker LIKE '%.NS%' as has_ns,
               (SELECT COUNT(*) FROM cash_flow_quarterly WHERE ticker = t.ticker) as quarterly_records
        FROM ticker t
        ORDER BY has_ns DESC, random()
        LIMIT 10
    """)
    
    print(f"Total tickers in database: {total_tickers}")
    print("\nSample tickers:")
    print("-" * 80)
    print(f"{'Ticker':<15} | Has .NS | Quarterly Records")
    print("-" * 80)
    for ticker, has_ns, count in cur.fetchall():
        print(f"{ticker:<15} | {str(has_ns):<6} | {count}")
    
    # Check if JSON files exist for tickers with/without .NS
    cur.execute("""
        SELECT 
            COUNT(*) as total_tickers,
            COUNT(CASE WHEN ticker LIKE '%.NS%' THEN 1 END) as with_ns,
            COUNT(CASE WHEN ticker NOT LIKE '%.NS%' THEN 1 END) as without_ns
        FROM ticker
    """)
    total, with_ns, without_ns = cur.fetchone()
    
    print(f"\nTicker format distribution:")
    print(f"- With .NS: {with_ns} ({with_ns/total:.1%})")
    print(f"- Without .NS: {without_ns} ({without_ns/total:.1%})")
    
    # Check if JSON files exist for tickers with/without .NS
    data_dir = Path("data/quarterly_cashflow")
    exists_with_ns = 0
    exists_without_ns = 0
    
    cur.execute("SELECT ticker FROM ticker")
    for (ticker,) in cur.fetchall():
        clean_ticker = ticker.replace('.NS', '')
        if (data_dir / f"{clean_ticker}.json").exists():
            if ticker.endswith('.NS'):
                exists_with_ns += 1
            else:
                exists_without_ns += 1
    
    print(f"\nJSON file existence:")
    print(f"- Exists for {exists_with_ns} tickers with .NS ({exists_with_ns/with_ns:.1%} of .NS tickers)" if with_ns > 0 else "")
    print(f"- Exists for {exists_without_ns} tickers without .NS ({exists_without_ns/without_ns:.1%} of non-.NS tickers)" if without_ns > 0 else "")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_ticker_formats()
