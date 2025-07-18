
import sys
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import List, Optional, Tuple
from tqdm import tqdm

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection  # type: ignore


class MomentumSignalCalculator:
    def __init__(self):
        self.db = DatabaseConnection()
        self.connection = self.db.connect()
        self.quarter_ends = self.generate_quarter_ends()
        self.lookbacks = {
            "momentum_3m": 63,
            "momentum_6m": 126,
            "momentum_12m": 252
        }
        self.fallback_thresholds = {
            63: 50,
            126: 100,
            252: 200
        }

    def generate_quarter_ends(self) -> List[date]:
        return [
            date(year, m, d)
            for year in range(2015, datetime.today().year + 1)
            for (m, d) in [(3, 31), (6, 30), (9, 30), (12, 31)]
        ]

    def get_tickers(self) -> List[str]:
        with self.connection.cursor() as cur:
            cur.execute("SELECT DISTINCT ticker FROM price_history ORDER BY ticker")
            return [row[0] for row in cur.fetchall()]

    def get_price_on_or_before(self, ticker: str, as_of_date: date) -> Optional[float]:
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

    def get_price_n_days_before(self, ticker: str, as_of_date: date, n_days: int) -> Optional[float]:
        start_date = as_of_date - timedelta(days=n_days + 90)
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT date, adjusted_close_price
                FROM price_history
                WHERE ticker = %s AND date BETWEEN %s AND %s
                      AND adjusted_close_price IS NOT NULL
                ORDER BY date ASC
            """, (ticker, start_date, as_of_date))
            rows = cur.fetchall()

            min_required = self.fallback_thresholds.get(n_days, int(n_days * 0.8))
            if len(rows) < min_required:
                return None
            return rows[min_required - 1][1]

    def get_avg_volume_3m(self, ticker: str, as_of_date: date) -> Optional[int]:
        start_date = as_of_date - timedelta(days=90)
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT AVG(volume)::BIGINT
                FROM price_history
                WHERE ticker = %s AND date BETWEEN %s AND %s AND volume IS NOT NULL
            """, (ticker, start_date, as_of_date))
            row = cur.fetchone()
            return row[0] if row else None

    def save_momentum_signals(self, data: List[Tuple]):
        if not data:
            return
        with self.connection.cursor() as cur:
            cur.executemany("""
                INSERT INTO momentum_signals (
                    ticker, as_of_date, momentum_3m, momentum_6m,
                    momentum_12m, avg_volume_3m
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, as_of_date) DO UPDATE SET
                    momentum_3m = EXCLUDED.momentum_3m,
                    momentum_6m = EXCLUDED.momentum_6m,
                    momentum_12m = EXCLUDED.momentum_12m,
                    avg_volume_3m = EXCLUDED.avg_volume_3m
            """, data)
            self.connection.commit()

    def process_all(self):
        tickers = self.get_tickers()
        total = 0
        stats = {
            "momentum_3m": 0,
            "momentum_6m": 0,
            "momentum_12m": 0,
            "avg_volume_3m": 0,
            "fully_valid": 0,
            "fully_invalid": 0,
        }

        for ticker in tqdm(tickers, desc="Processing tickers"):
            for qend in self.quarter_ends:
                current_price = self.get_price_on_or_before(ticker, qend)
                price_3m = self.get_price_n_days_before(ticker, qend, self.lookbacks["momentum_3m"])
                price_6m = self.get_price_n_days_before(ticker, qend, self.lookbacks["momentum_6m"])
                price_12m = self.get_price_n_days_before(ticker, qend, self.lookbacks["momentum_12m"])
                avg_vol = self.get_avg_volume_3m(ticker, qend)

                if current_price is None:
                    stats["fully_invalid"] += 1
                    continue

                missing = False
                if price_3m is None:
                    stats["momentum_3m"] += 1
                    missing = True
                if price_6m is None:
                    stats["momentum_6m"] += 1
                    missing = True
                if price_12m is None:
                    stats["momentum_12m"] += 1
                    missing = True
                if avg_vol is None:
                    stats["avg_volume_3m"] += 1
                    missing = True

                if missing:
                    stats["fully_invalid"] += 1
                    continue

                try:
                    row = (
                        ticker,
                        qend,
                        round((current_price / price_3m - 1), 4),
                        round((current_price / price_6m - 1), 4),
                        round((current_price / price_12m - 1), 4),
                        avg_vol
                    )
                    self.save_momentum_signals([row])
                    stats["fully_valid"] += 1
                    total += 1
                except Exception:
                    stats["fully_invalid"] += 1
                    continue

        print("[SUMMARY] Momentum Signal Stats:")
        for k, v in stats.items():
            print(f"{k}: {v}")
        print(f"Total valid records saved: {total}")


def main():
    try:
        calc = MomentumSignalCalculator()
        calc.process_all()
    except Exception as e:
        print("[ERROR]", str(e))


if __name__ == "__main__":
    main()
