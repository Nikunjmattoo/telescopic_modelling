"""
compute_target_pe.py

Computes target P/E ratio for each fiscal year by using valuation_snapshots (price and ttm_eps).
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import List, Tuple
from tqdm import tqdm

# Add root directory to path
sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection

class TargetPECalculator:
    def __init__(self):
        self.db = DatabaseConnection()
        self.conn = self.db.connect()

    def fetch_required_records(self) -> List[Tuple]:
        """Fetch records that need target_pe and can be computed."""
        query = """
            SELECT
                d.ticker,
                d.fiscal_year,
                d.period_ending,
                v.ttm_eps,
                v.entry_price
            FROM derived_metrics d
            JOIN valuation_snapshots v
                ON d.ticker = v.ticker
            WHERE d.target_pe IS NULL
              AND v.ttm_eps IS NOT NULL AND v.ttm_eps > 0
              AND v.entry_price IS NOT NULL AND v.entry_price > 0
              AND v.as_of_date <= d.period_ending
        """
        with self.conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
            print(f"[INFO] Fetched {len(results)} records for target_pe computation.")
            return results

    def compute_and_save(self, rows: List[Tuple]):
        """Compute target_pe and persist to derived_metrics."""
        updates = []

        for ticker, fiscal_year, period_ending, ttm_eps, entry_price in tqdm(rows, desc="Computing target_pe"):
            try:
                target_pe = entry_price / ttm_eps
                updates.append((round(target_pe, 2), ticker, fiscal_year))
            except Exception as e:
                print(f"[WARN] Failed for {ticker}-{fiscal_year}: {str(e)}")

        if not updates:
            print("[WARN] No valid updates to apply.")
            return

        with self.conn.cursor() as cur:
            cur.executemany("""
                UPDATE derived_metrics
                SET target_pe = %s, last_updated = CURRENT_TIMESTAMP
                WHERE ticker = %s AND fiscal_year = %s
            """, updates)
            self.conn.commit()
            print(f"[OK] target_pe values updated: {len(updates)}")

    def run(self):
        rows = self.fetch_required_records()
        if not rows:
            print("[ERROR] No eligible records to update.")
            return
        self.compute_and_save(rows)

def main():
    print("=== Starting target_pe computation ===")
    try:
        calc = TargetPECalculator()
        calc.run()
    except Exception as e:
        import traceback
        print(f"[FATAL] {str(e)}")
        traceback.print_exc()
        return 1
    print("=== All done ===")
    return 0

if __name__ == "__main__":
    sys.exit(main())
