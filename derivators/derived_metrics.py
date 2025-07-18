"""
derived_metrics.py

This module handles the derivation of metrics for the derived_metrics table.
It processes raw data from various sources to calculate derived financial metrics.
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import psycopg
from tqdm import tqdm

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection

class DerivedMetricsCalculator:
    """Handles calculation and storage of derived financial metrics."""
    
    def __init__(self):
        """Initialize the calculator with database connection."""
        self.db = DatabaseConnection()
        self.db.connection = self.db.connect()  # Explicitly connect
    
    def get_tickers(self) -> List[str]:
        """Get list of all unique tickers from source tables."""
        cur = self.db.connection.cursor()
        try:
            cur.execute("""
                SELECT DISTINCT ticker FROM (
                    SELECT ticker FROM income_statement_annual
                    UNION
                    SELECT ticker FROM balance_sheet_annual
                    UNION
                    SELECT ticker FROM cash_flow_annual
                ) t
                ORDER BY ticker
            """)
            return [row[0] for row in cur.fetchall()]  # Access by index instead of name
        finally:
            cur.close()
    
    def calculate_metrics(self, ticker: str) -> List[Dict[str, Any]]:
        """
        Calculate derived metrics for a single ticker.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            List of metric records to be inserted
        """
        cur = self.db.connection.cursor()
        try:
            # Get all required data in a single query
            cur.execute("""
                SELECT 
                    i.period_ending,
                    i.diluted_eps,
                    i.total_revenue,
                    i.net_income,
                    i.operating_income,
                    b.stockholders_equity,
                    b.total_assets,
                    b.total_debt,
                    b.current_assets,
                    b.current_liabilities,
                    c.operating_cash_flow,
                    c.free_cash_flow,
                    c.dividends_paid
                FROM income_statement_annual i
                LEFT JOIN balance_sheet_annual b 
                    ON i.ticker = b.ticker AND i.period_ending = b.period_ending
                LEFT JOIN cash_flow_annual c
                    ON i.ticker = c.ticker AND i.period_ending = c.period_ending
                WHERE i.ticker = %s
                ORDER BY i.period_ending
            """, (ticker,))
            
            rows = cur.fetchall()
            if not rows:
                return []
                
            # Prepare metrics for insertion
            metrics = []
            for row in rows:
                # Skip if any required field is missing
                if any(field is None for field in row):
                    continue
                
                # Map columns by position
                (
                    period_ending,
                    diluted_eps,
                    total_revenue,
                    net_income,
                    operating_income,
                    stockholders_equity,
                    total_assets,
                    total_debt,
                    current_assets,
                    current_liabilities,
                    operating_cash_flow,
                    free_cash_flow,
                    dividends_paid
                ) = row
                    
                # For March 31st year-end, fiscal year is previous calendar year
                # e.g., March 31, 2023 → FY22 (2022-23)
                fiscal_year = period_ending.year - 1
                
                metrics.append({
                    'ticker': ticker,
                    'fiscal_year': fiscal_year,
                    'period_ending': period_ending,
                    'eps': diluted_eps,
                    'revenue': total_revenue,
                    'net_income': net_income,
                    'operating_income': operating_income,
                    'stockholders_equity': stockholders_equity,
                    'total_assets': total_assets,
                    'total_debt': total_debt,
                    'current_assets': current_assets,
                    'current_liabilities': current_liabilities,
                    'operating_cash_flow': operating_cash_flow,
                    'free_cash_flow': free_cash_flow,
                    'dividends_paid': dividends_paid,
                    'last_updated': datetime.now()
                })
            
            return metrics
        finally:
            cur.close()
    
    def save_metrics(self, metrics: List[Dict[str, Any]]) -> int:
        """
        Save calculated metrics to the database.
        Following the same pattern as extract_cashflow_quarterly.py
        """
        if not metrics:
            return 0
            
        conn = self.db.connection
        cur = conn.cursor()
        inserted = 0
        
        try:
            data = []
            for m in metrics:
                # Convert metric dict to tuple in the correct order
                row = (
                    m['ticker'],
                    m['fiscal_year'],  # Using fiscal_year instead of period_ending for the second column
                    m['eps'],
                    m['revenue'],
                    m['net_income'],
                    m['operating_income'],
                    m['stockholders_equity'],
                    m['total_assets'],
                    m['total_debt'],
                    m['current_assets'],
                    m['current_liabilities'],
                    m['operating_cash_flow'],
                    m['free_cash_flow'],
                    m['dividends_paid'],
                    m['period_ending']  # Adding period_ending as the last column
                )
                data.append(row)
            
            # Use executemany with ON CONFLICT for upsert
            cur.executemany("""
                INSERT INTO derived_metrics (
                    ticker, fiscal_year,
                    eps, revenue, net_income, operating_income,
                    stockholders_equity, total_assets, total_debt,
                    current_assets, current_liabilities,
                    operating_cash_flow, free_cash_flow, dividends_paid,
                    period_ending, last_updated
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (ticker, fiscal_year) DO UPDATE SET
                    eps = EXCLUDED.eps,
                    revenue = EXCLUDED.revenue,
                    net_income = EXCLUDED.net_income,
                    operating_income = EXCLUDED.operating_income,
                    stockholders_equity = EXCLUDED.stockholders_equity,
                    total_assets = EXCLUDED.total_assets,
                    total_debt = EXCLUDED.total_debt,
                    current_assets = EXCLUDED.current_assets,
                    current_liabilities = EXCLUDED.current_liabilities,
                    operating_cash_flow = EXCLUDED.operating_cash_flow,
                    free_cash_flow = EXCLUDED.free_cash_flow,
                    dividends_paid = EXCLUDED.dividends_paid,
                    last_updated = EXCLUDED.last_updated
            """, data)
            
            inserted = cur.rowcount
            conn.commit()
            return inserted
            
        except Exception as e:
            conn.rollback()
            print(f"Error saving metrics: {str(e)}")
            raise
            
        finally:
            cur.close()
    
    def process_all_tickers(self):
        """Process all tickers and save derived metrics."""
        tickers = self.get_tickers()
        total_processed = 0
        
        with tqdm(tickers, desc="Processing tickers") as pbar:
            for ticker in pbar:
                try:
                    metrics = self.calculate_metrics(ticker)
                    if metrics:
                        count = self.save_metrics(metrics)
                        total_processed += count
                        pbar.set_postfix({"Processed": f"{total_processed} records"})
                except Exception as e:
                    print(f"\nError processing {ticker}: {str(e)}")
                    continue
        
        return total_processed

def main():
    """Main function to run the metrics calculation."""
    try:
        print("Starting derived metrics calculation...")
        calculator = DerivedMetricsCalculator()
        print("Successfully initialized calculator")
        print(f"Database connection: {calculator.db.connection}")
        total = calculator.process_all_tickers()
        print(f"\nCompleted! Processed {total} records in total.")
    except Exception as e:
        import traceback
        print(f"Error in main: {str(e)}")
        print("\nStack trace:")
        traceback.print_exc()
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
