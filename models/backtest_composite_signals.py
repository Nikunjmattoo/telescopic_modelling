import sys
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
from scipy.stats import spearmanr
from tqdm import tqdm

sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection


class BacktestCompositeSignal:
    def __init__(self):
        self.db = DatabaseConnection()
        self.connection = self.db.connect()
        self.holding_periods = [30, 90, 180]
        self.top_n = 10

    def get_composite_scores(self):
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT as_of_date, ticker, composite_score, rank
                FROM composite_signals
                WHERE as_of_date >= '2015-01-01'
            """)
            return cur.fetchall()

    def get_price(self, ticker, dt):
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT adjusted_close_price
                FROM price_history
                WHERE ticker = %s AND date >= %s
                ORDER BY date ASC
                LIMIT 1
            """, (ticker, dt))
            row = cur.fetchone()
            return row[0] if row else None

    def save_detail(self, rows):
        with self.connection.cursor() as cur:
            print(f"[INFO] Inserting {len(rows)} rows into composite_signal_backtest_detail...")
            cur.executemany("""
                INSERT INTO composite_signal_backtest_detail (
                    as_of_date, holding_days, ticker, composite_score,
                    predicted_rank, actual_return, actual_rank,
                    matched_top_n, matched_bottom_n
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (as_of_date, holding_days, ticker) DO UPDATE SET
                    actual_return = EXCLUDED.actual_return,
                    actual_rank = EXCLUDED.actual_rank,
                    matched_top_n = EXCLUDED.matched_top_n,
                    matched_bottom_n = EXCLUDED.matched_bottom_n,
                    last_updated = CURRENT_TIMESTAMP
            """, rows)
        self.connection.commit()

    def save_summary(self, rows):
        with self.connection.cursor() as cur:
            print(f"[INFO] Inserting summary rows into composite_signal_backtest_summary...")
            cur.executemany("""
                INSERT INTO composite_signal_backtest_summary (
                    as_of_date, holding_days, top_n,
                    spearman_corr, top_n_overlap, n_eligible,
                    avg_return_top, avg_return_bottom
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (as_of_date, holding_days, top_n) DO UPDATE SET
                    spearman_corr = EXCLUDED.spearman_corr,
                    top_n_overlap = EXCLUDED.top_n_overlap,
                    n_eligible = EXCLUDED.n_eligible,
                    avg_return_top = EXCLUDED.avg_return_top,
                    avg_return_bottom = EXCLUDED.avg_return_bottom,
                    last_updated = CURRENT_TIMESTAMP
            """, rows)
        self.connection.commit()

    def run(self):
        scores = self.get_composite_scores()
        grouped = {}
        for as_of_date, ticker, score, rank in scores:
            grouped.setdefault(as_of_date, []).append((ticker, score, rank))

        for as_of_date, entries in tqdm(grouped.items(), desc="Backtesting"):
            print(f"[INFO] Processing date: {as_of_date}")
            tickers = [e[0] for e in entries]
            score_map = {e[0]: (e[1], e[2]) for e in entries}  # ticker -> (score, predicted_rank)

            for h in self.holding_periods:
                end_date = as_of_date + timedelta(days=h)
                print(f"  [INFO] Holding period: {h} days")
                actual_returns = []

                for ticker in tickers:
                    try:
                        price_start = self.get_price(ticker, as_of_date)
                        price_end = self.get_price(ticker, end_date)
                        print(f"  [INFO] Ticker: {ticker}")
                        print(f"    - Price on {as_of_date}: {'FOUND' if price_start else 'MISSING'}")
                        print(f"    - Price on {end_date}: {'FOUND' if price_end else 'MISSING'}")
                        print(f"    - price_start: {price_start}")
                        print(f"    - price_end: {price_end}")
                        if price_start and price_end and price_start != 0:
                            ret = (price_end - price_start) / price_start
                            actual_returns.append((ticker, ret))
                    except Exception as e:
                        print(f"  [ERROR] Failed for {ticker}: {e}")
                        continue

                print(f"  [INFO] Found {len(actual_returns)} return records")
                if len(actual_returns) < self.top_n * 2:
                    print("  [WARN] Skipping due to insufficient data")
                    continue

                actual_returns.sort(key=lambda x: x[1], reverse=True)
                actual_rank = {t: r + 1 for r, (t, _) in enumerate(actual_returns)}
                detail_rows = []
                pred_ranks = []
                actual_ranks = []
                top_n_tickers = set([t for t, _ in sorted(score_map.items(), key=lambda x: -x[1][0])[:self.top_n]])
                bottom_n_tickers = set([t for t, _ in sorted(score_map.items(), key=lambda x: x[1][0])[:self.top_n]])

                for ticker, ret in actual_returns:
                    pred_score, pred_rank = score_map.get(ticker, (None, None))
                    act_rank = actual_rank[ticker]
                    if pred_score is None:
                        continue
                    matched_top = ticker in top_n_tickers
                    matched_bottom = ticker in bottom_n_tickers
                    pred_ranks.append(pred_rank)
                    actual_ranks.append(act_rank)
                    detail_rows.append((
                        as_of_date, h, ticker, pred_score, pred_rank,
                        ret, act_rank, matched_top, matched_bottom
                    ))

                self.save_detail(detail_rows)

                spearman_corr = round(spearmanr(pred_ranks, actual_ranks).correlation, 4)
                overlap = sum(1 for r in detail_rows if r[7])
                top_avg = round(np.mean([r[5] for r in detail_rows if r[7]]), 4)
                bottom_avg = round(np.mean([r[5] for r in detail_rows if r[8]]), 4)
                summary_row = [(as_of_date, h, self.top_n, spearman_corr, overlap,
                                len(actual_returns), top_avg, bottom_avg)]
                self.save_summary(summary_row)


def main():
    try:
        bt = BacktestCompositeSignal()
        bt.run()
    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":
    main()
