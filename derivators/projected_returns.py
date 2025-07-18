"""
compute_projected_returns.py

This module calculates and stores projected EPS, price, and return for each stock
based on historical snapshot data and derived metrics.
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import List, Tuple
from tqdm import tqdm

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection

class ProjectedReturnCalculator:
    """Handles projection of future returns based on EPS CAGR and target P/E."""
    
    def __init__(self):
        self.db = DatabaseConnection()
        self.conn = self.db.connect()
    
    def get_input_data(self) -> List[Tuple]:
        """Fetch required snapshot and derived inputs from the database."""
        print("Fetching input data from valuation_snapshots and derived_metrics...")

        query = """
            SELECT
                v.ticker,
                v.as_of_date,
                v.ttm_eps,
                v.entry_price,
                d.eps_cagr_2y,
                d.target_pe
            FROM valuation_snapshots v
            JOIN (
                SELECT DISTINCT ON (ticker) *
                FROM derived_metrics
                WHERE eps_cagr_2y IS NOT NULL AND target_pe IS NOT NULL
                ORDER BY ticker, fiscal_year DESC
            ) d ON v.ticker = d.ticker
            WHERE
                v.ttm_eps IS NOT NULL AND v.ttm_eps > 0
                AND v.entry_price IS NOT NULL AND v.entry_price > 0
        """

        with self.conn.cursor() as cur:
            cur.execute(query)
            data = cur.fetchall()
            print(f"[OK] Total valid input rows: {len(data)}")
            return data

    def compute_projections(self, rows: List[Tuple]) -> List[Tuple]:
        """Compute projected EPS, price, and return for each row."""
        print("Computing projections...")
        results = []

        for row in tqdm(rows, desc="Projecting"):
            ticker, as_of_date, ttm_eps, entry_price, eps_cagr, target_pe = row
            try:
                projected_eps = ttm_eps * ((1 + eps_cagr) ** 2)
                projected_price = projected_eps * target_pe
                projected_return = (projected_price / entry_price) - 1

                results.append((
                    ticker,
                    as_of_date,
                    round(projected_eps, 4),
                    round(projected_price, 2),
                    round(projected_return, 4),
                    round(eps_cagr, 4),
                    round(target_pe, 2),
                ))
            except Exception as e:
                print(f"[ERROR] Failed for {ticker} on {as_of_date}: {str(e)}")

        print(f"[OK] Computation complete. Valid records: {len(results)}")
        return results

    def save_to_db(self, records: List[Tuple]):
        """Insert computed projections into the projected_returns table."""
        if not records:
            print("[WARN] No records to insert.")
            return 0

        print("Saving projections to database...")

        with self.conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO projected_returns (
                    ticker, as_of_date,
                    projected_eps, projected_price, projected_return,
                    eps_cagr_used, target_pe_used, last_updated
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (ticker, as_of_date) DO UPDATE SET
                    projected_eps = EXCLUDED.projected_eps,
                    projected_price = EXCLUDED.projected_price,
                    projected_return = EXCLUDED.projected_return,
                    eps_cagr_used = EXCLUDED.eps_cagr_used,
                    target_pe_used = EXCLUDED.target_pe_used,
                    last_updated = EXCLUDED.last_updated
            """, records)
            self.conn.commit()

        print(f"[OK] Projections saved: {len(records)}")
        return len(records)

    def run(self):
        """Main pipeline for fetching, computing, and saving projections."""
        all_input = self.get_input_data()
        if not all_input:
            print("[ERROR] No valid input rows found. Exiting.")
            return
        computed = self.compute_projections(all_input)
        self.save_to_db(computed)

def main():
    print("=== Starting projected returns computation ===")
    try:
        job = ProjectedReturnCalculator()
        job.run()
    except Exception as e:
        import traceback
        print(f"[FATAL ERROR] {str(e)}")
        traceback.print_exc()
        return 1
    print("=== All done ===")
    return 0

if __name__ == "__main__":
    sys.exit(main())
