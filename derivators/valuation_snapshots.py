import sys
from pathlib import Path
from datetime import datetime, date
from typing import List, Optional, Tuple
from tqdm import tqdm

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection


class ValuationSnapshotCalculator:
    def __init__(self):
        self.db = DatabaseConnection()
        self.connection = self.db.connect()
        self.quarter_ends = self.generate_quarter_ends()

    def generate_quarter_ends(self) -> List[date]:
        quarter_ends = []
        for year in range(2015, datetime.today().year + 1):
            quarter_ends.extend([
                date(year, 3, 31),
                date(year, 6, 30),
                date(year, 9, 30),
                date(year, 12, 31)
            ])
        return quarter_ends

    def get_tickers(self) -> List[str]:
        with self.connection.cursor() as cur:
            cur.execute("SELECT DISTINCT ticker FROM income_statement_quarterly ORDER BY ticker")
            return [row[0] for row in cur.fetchall()]

    def get_strict_ttm_eps(self, ticker: str, as_of_date: date) -> Optional[Tuple[float, bool]]:
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT diluted_eps
                FROM income_statement_quarterly
                WHERE ticker = %s AND period_ending <= %s AND diluted_eps IS NOT NULL
                ORDER BY period_ending DESC
                LIMIT 4
            """, (ticker, as_of_date))
            rows = cur.fetchall()
            eps_values = [r[0] for r in rows]

            if len(eps_values) < 2:
                return None  # Too few quarters to estimate

            is_complete = len(eps_values) == 4
            avg_eps = sum(eps_values) / len(eps_values)
            estimated_total = sum(eps_values) + avg_eps * (4 - len(eps_values))

            return round(estimated_total, 4), is_complete

    def get_entry_price(self, ticker: str, as_of_date: date) -> Optional[float]:
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT adjusted_close_price
                FROM price_history
                WHERE ticker = %s AND date <= %s
                ORDER BY date DESC
                LIMIT 1
            """, (ticker, as_of_date))
            row = cur.fetchone()
            return row[0] if row else None

    def save_snapshots(self, data: List[Tuple], ticker: str):
        if not data:
            print(f"[SKIP] {ticker} - No rows to insert.")
            return
        try:
            with self.connection.cursor() as cur:
                cur.executemany("""
                    INSERT INTO valuation_snapshots (
                        ticker, as_of_date, ttm_eps, ttm_eps_complete,
                        entry_price, ttm_pe, snapshot_date, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (ticker, as_of_date) DO UPDATE SET
                        ttm_eps = EXCLUDED.ttm_eps,
                        ttm_eps_complete = EXCLUDED.ttm_eps_complete,
                        entry_price = EXCLUDED.entry_price,
                        ttm_pe = EXCLUDED.ttm_pe,
                        snapshot_date = EXCLUDED.snapshot_date,
                        last_updated = EXCLUDED.last_updated
                """, data)
                self.connection.commit()
            print(f"[DB] {ticker} - Saved {len(data)} snapshot rows")
        except Exception as e:
            print(f"[ERROR] DB insert failed for {ticker}: {e}")

    def process_ticker(self, ticker: str) -> int:
        rows = []
        for qend in self.quarter_ends:
            result = self.get_strict_ttm_eps(ticker, qend)
            if result is None:
                continue

            ttm_eps, is_complete = result
            price = self.get_entry_price(ticker, qend)
            if price is None:
                continue

            ttm_pe = price / ttm_eps if ttm_eps != 0 else None
            rows.append((
                ticker,
                qend,
                ttm_eps,
                is_complete,
                round(price, 2),
                round(ttm_pe, 2) if ttm_pe else None,
                qend
            ))

        self.save_snapshots(rows, ticker)
        print(f"[{ticker}] Processed {len(rows)} rows")
        return len(rows)

    def process_all(self):
        tickers = self.get_tickers()
        total = 0
        for ticker in tqdm(tickers, desc="Processing tickers"):
            try:
                count = self.process_ticker(ticker)
                total += count
            except Exception as e:
                print(f"[ERROR] Error processing {ticker}: {e}")
        print(f"[OK] Completed. Total snapshots saved: {total}")


def main():
    try:
        calc = ValuationSnapshotCalculator()
        calc.process_all()
    except Exception as e:
        print("[ERROR]", str(e))


if __name__ == "__main__":
    main()
