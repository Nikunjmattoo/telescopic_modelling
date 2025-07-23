import sys
from pathlib import Path
from datetime import timedelta
import numpy as np

sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection


class BacktestCompositeSignalAll:
    def __init__(self):
        self.db = DatabaseConnection()
        self.connection = self.db.connect()
        self.holding_periods = [30, 90, 180]

    def get_composite_scores(self):
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT as_of_date, ticker, composite_score
                FROM composite_signals
                WHERE as_of_date >= '2015-01-01'
            """)
            return cur.fetchall()

    def get_prev_trading_day(self, ticker, target_date):
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT date
                FROM price_history
                WHERE ticker = %s AND date <= %s
                ORDER BY date DESC
                LIMIT 1
            """, (ticker, target_date))
            row = cur.fetchone()
            return row[0] if row else None

    def get_next_trading_day(self, ticker, target_date):
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT date
                FROM price_history
                WHERE ticker = %s AND date >= %s
                ORDER BY date ASC
                LIMIT 1
            """, (ticker, target_date))
            row = cur.fetchone()
            return row[0] if row else None

    def get_price(self, ticker, dt):
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT adjusted_close_price
                FROM price_history
                WHERE ticker = %s AND date = %s
            """, (ticker, dt))
            row = cur.fetchone()
            return row[0] if row else None

    def save_rows(self, rows):
        with self.connection.cursor() as cur:
            cur.executemany("""
                INSERT INTO composite_signal_backtest (
                    as_of_date, holding_days, ticker, composite_score,
                    price_start_date, price_end_date, price_start, price_end, return_value
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (as_of_date, holding_days, ticker) DO UPDATE SET
                    price_start = EXCLUDED.price_start,
                    price_end = EXCLUDED.price_end,
                    return_value = EXCLUDED.return_value,
                    last_updated = CURRENT_TIMESTAMP
            """, rows)
        self.connection.commit()

    def run(self):
        scores = self.get_composite_scores()
        grouped = {}
        for as_of_date, ticker, score in scores:
            grouped.setdefault(as_of_date, []).append((ticker, score))

        for as_of_date, entries in grouped.items():
            print(f"[INFO] Processing {as_of_date} with {len(entries)} tickers")

            for h in self.holding_periods:
                rows_to_insert = []
                for ticker, score in entries:
                    start_date = self.get_prev_trading_day(ticker, as_of_date)
                    end_target = as_of_date + timedelta(days=h)
                    end_date = self.get_next_trading_day(ticker, end_target)

                    price_start = self.get_price(ticker, start_date) if start_date else None
                    price_end = self.get_price(ticker, end_date) if end_date else None

                    if price_start and price_end and price_start != 0:
                        ret = (price_end - price_start) / price_start
                        rows_to_insert.append((
                            as_of_date, h, ticker, score,
                            start_date, end_date, price_start, price_end, round(ret, 6)
                        ))

                print(f"  [INFO] Holding {h}d: {len(rows_to_insert)} tickers with valid data")
                self.save_rows(rows_to_insert)


def main():
    bt = BacktestCompositeSignalAll()
    bt.run()


if __name__ == "__main__":
    main()
