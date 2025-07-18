"""
compute_eps_cagr.py
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, List
from collections import defaultdict
from tqdm import tqdm
import psycopg

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection

def fetch_eps_data() -> Dict[str, List[Tuple[str, float]]]:
    """Fetch EPS and fiscal year data for all tickers from derived_metrics."""
    db = DatabaseConnection()
    conn = db.connect()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT ticker, fiscal_year, eps
            FROM derived_metrics
            WHERE eps IS NOT NULL
        """)
        rows = cur.fetchall()
        data = defaultdict(list)
        for ticker, fiscal_year, eps in rows:
            data[ticker.strip()] = data[ticker.strip()] + [(str(fiscal_year), float(eps))]
        return data
    finally:
        cur.close()
        conn.close()

def update_eps_cagr(records: List[Tuple[float, str, str]]) -> int:
    """Batch update derived_metrics with eps_cagr_2y."""
    if not records:
        print("[INFO] No records to update.")
        return 0

    db = DatabaseConnection()
    conn = db.connect()
    cur = conn.cursor()

    print(f"[DEBUG] Attempting to update {len(records)} rows...")

    try:
        cur.executemany("""
            UPDATE derived_metrics
            SET eps_cagr_2y = %s, last_updated = CURRENT_TIMESTAMP
            WHERE ticker = %s AND fiscal_year = %s
        """, records)
        conn.commit()
        return cur.rowcount
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to update CAGR: {str(e)}")
        return 0
    finally:
        cur.close()
        conn.close()

def compute_eps_cagr():
    data = fetch_eps_data()

    expected = 0
    skipped_due_to_missing = 0
    skipped_due_to_zero_or_negative = 0
    updates = []

    for ticker, rows in tqdm(data.items(), desc="Computing EPS CAGR"):
        rows = sorted(rows, key=lambda r: r[0])  # sort by fiscal_year
        eps_by_year = {fy: eps for fy, eps in rows}
        fiscal_years = sorted(eps_by_year.keys())

        for i in range(2, len(fiscal_years)):
            fy = fiscal_years[i]
            fy_prev = fiscal_years[i - 2]

            eps = eps_by_year.get(fy)
            eps_2y_ago = eps_by_year.get(fy_prev)

            if eps is None or eps_2y_ago is None:
                skipped_due_to_missing += 1
                continue

            expected += 1
            if eps <= 0 or eps_2y_ago <= 0:
                skipped_due_to_zero_or_negative += 1
                continue

            cagr = ((eps / eps_2y_ago) ** (1 / 2)) - 1
            updates.append((cagr, ticker, fy))

    updated = update_eps_cagr(updates)

    print("\n=== EPS CAGR Computation Summary ===")
    print(f"Total tickers processed        : {len(data)}")
    print(f"Expected computations          : {expected}")
    print(f"Skipped due to missing EPS     : {skipped_due_to_missing}")
    print(f"Skipped due to EPS <= 0         : {skipped_due_to_zero_or_negative}")
    print(f"Updates prepared               : {len(updates)}")
    print(f"[OK] Rows successfully updated : {updated}")

    if updated == 0 and len(updates) > 0:
        print("[WARNING] No rows updated. Likely cause: fiscal_year type mismatch (int vs text).")

if __name__ == "__main__":
    compute_eps_cagr()
