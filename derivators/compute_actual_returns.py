"""
compute_actual_returns.py

Calculates actual EPS and price for each projected stock and computes realized returns
after a 2-year horizon from the as_of_date of projection.
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple, Dict
from tqdm import tqdm

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection

class ActualReturnsCalculator:
    def __init__(self):
        self.db = DatabaseConnection()
        self.conn = self.db.connect()

    def fetch_projection_inputs(self) -> List[Tuple]:
        """
        Fetch records from projected_returns and entry price from valuation_snapshots.
        """
        print("Fetching projected return entries...")
        query = """
            SELECT 
                p.ticker,
                p.as_of_date,
                p.projected_return,
                v.entry_price
            FROM projected_returns p
            JOIN valuation_snapshots v
              ON p.ticker = v.ticker AND p.as_of_date = v.as_of_date
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            print(f"[OK] Total projection records found: {len(rows)}")
            return rows

    def fetch_eps_quarters(self, ticker: str, start_date: datetime, end_date: datetime) -> List[float]:
        query = """
            SELECT diluted_eps
            FROM income_statement_quarterly
            WHERE ticker = %s
              AND period_ending > %s AND period_ending <= %s
            ORDER BY period_ending
            LIMIT 4
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (ticker, start_date, end_date))
            return [r[0] for r in cur.fetchall() if r[0] is not None]

    def fetch_price_on_or_before(self, ticker: str, target_date: datetime) -> float:
        query = """
            SELECT adjusted_close_price
            FROM price_history
            WHERE ticker = %s AND date <= %s
            ORDER BY date DESC
            LIMIT 1
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (ticker, target_date))
            row = cur.fetchone()
            return row[0] if row else None

    def save_actual_returns(self, records: List[Tuple]):
        if not records:
            print("[WARN] No actual return records to save.")
            return 0
        print(f"Saving {len(records)} actual return records to DB...")
        with self.conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO actual_returns (
                    ticker, as_of_date,
                    actual_eps, actual_price, actual_return,
                    last_updated
                )
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (ticker, as_of_date) DO UPDATE SET
                    actual_eps = EXCLUDED.actual_eps,
                    actual_price = EXCLUDED.actual_price,
                    actual_return = EXCLUDED.actual_return,
                    last_updated = EXCLUDED.last_updated
            """, records)
            self.conn.commit()
        print("[OK] Actual returns saved.")

    def compute_all(self):
        rows = self.fetch_projection_inputs()
        records = []
        skipped = 0

        for ticker, as_of_date, projected_return, entry_price in tqdm(rows, desc="Computing actuals"):
            end_date = as_of_date + timedelta(days=730)  # 2 years approx
            try:
                eps_vals = self.fetch_eps_quarters(ticker, as_of_date, end_date)
                if len(eps_vals) < 4:
                    skipped += 1
                    continue
                actual_eps = sum(eps_vals)

                actual_price = self.fetch_price_on_or_before(ticker, end_date)
                if actual_price is None:
                    skipped += 1
                    continue

                actual_return = (actual_price / entry_price) - 1

                records.append((
                    ticker,
                    as_of_date,
                    round(actual_eps, 4),
                    round(actual_price, 2),
                    round(actual_return, 4)
                ))
            except Exception as e:
                print(f"[ERROR] {ticker} @ {as_of_date}: {str(e)}")
                skipped += 1

        print(f"[SUMMARY] Computed: {len(records)}, Skipped: {skipped}")
        self.save_actual_returns(records)

def main():
    print("=== Starting actual return computation ===")
    try:
        job = ActualReturnsCalculator()
        job.compute_all()
    except Exception as e:
        import traceback
        print(f"[FATAL ERROR] {str(e)}")
        traceback.print_exc()
        return 1
    print("=== All done ===")
    return 0

if __name__ == "__main__":
    sys.exit(main())
