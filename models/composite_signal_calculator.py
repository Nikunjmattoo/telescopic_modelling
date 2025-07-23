import sys
from pathlib import Path
from datetime import datetime, date
from typing import List, Optional, Tuple
from tqdm import tqdm
import numpy as np

sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection

class CompositeSignalCalculator:
    def __init__(self):
        self.db = DatabaseConnection()
        self.connection = self.db.connect()
        self.quarter_ends = self.generate_quarter_ends()

    def generate_quarter_ends(self) -> List[date]:
        return [
            date(year, m, d)
            for year in range(2015, datetime.today().year + 1)
            for (m, d) in [(3, 31), (6, 30), (9, 30), (12, 31)]
        ]

    def get_composite_inputs(self, as_of_date: date) -> List[Tuple]:
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT
                    v.ticker,
                    v.valuation_signal,
                    m.momentum_3m,
                    m.momentum_6m,
                    m.momentum_12m,
                    f.eps_growth,
                    f.revenue_growth,
                    f.roe,
                    f.net_margin
                FROM valuation_signals v
                JOIN momentum_signals m ON v.ticker = m.ticker AND v.as_of_date = m.as_of_date
                JOIN fundamental_scores f ON v.ticker = f.ticker AND v.as_of_date = f.as_of_date
                WHERE v.as_of_date = %s
            """, (as_of_date,))
            return cur.fetchall()

    def save_scores(self, data: List[Tuple]):
        if not data:
            return
        with self.connection.cursor() as cur:
            cur.executemany("""
                INSERT INTO composite_signals (
                    ticker, as_of_date, composite_score,
                    rank, percentile, last_updated
                ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (ticker, as_of_date) DO UPDATE SET
                    composite_score = EXCLUDED.composite_score,
                    rank = EXCLUDED.rank,
                    percentile = EXCLUDED.percentile,
                    last_updated = CURRENT_TIMESTAMP
            """, data)
            self.connection.commit()

    def compute_zscores(self, arr: List[float]) -> List[float]:
        arr_np = np.array(arr)
        mean = np.mean(arr_np)
        std = np.std(arr_np)
        return [(x - mean) / std if std != 0 else 0 for x in arr_np]

    def process_quarter(self, as_of_date: date) -> int:
        raw_records = self.get_composite_inputs(as_of_date)
        if not raw_records:
            return 0

        tickers = []
        feature_keys = [
            "valuation_signal", "momentum_3m", "momentum_6m", "momentum_12m",
            "eps_growth", "revenue_growth", "roe", "net_margin"
        ]
        features = {key: [] for key in feature_keys}

        for row in raw_records:
            if None in row[1:]:
                continue
            tickers.append(row[0])
            for j, key in enumerate(feature_keys):
                features[key].append(float(row[j + 1]))

        if not tickers:
            return 0

        z = {k: self.compute_zscores(v) for k, v in features.items()}

        scores = []
        for i in range(len(tickers)):
            score = (
                z["eps_growth"][i] +
                z["revenue_growth"][i] +
                z["roe"][i] +
                z["net_margin"][i] +
                z["momentum_3m"][i] +
                z["momentum_6m"][i] +
                z["momentum_12m"][i] -
                z["valuation_signal"][i]
            )
            scores.append((tickers[i], round(score, 4)))

        scores.sort(key=lambda x: x[1], reverse=True)
        total = len(scores)
        data_to_save = []
        for rank, (ticker, score) in enumerate(scores, start=1):
            percentile = round((total - rank) / (total - 1), 4) if total > 1 else 0
            data_to_save.append((ticker, as_of_date, score, rank, percentile))

        self.save_scores(data_to_save)
        return total

    def process_all(self):
        total = 0
        for qend in tqdm(self.quarter_ends, desc="Processing composite signals"):
            try:
                count = self.process_quarter(qend)
                total += count
            except Exception as e:
                print(f"Error processing {qend}: {e}")
        print(f"[OK] Completed. Total composite signals saved: {total}")

def main():
    try:
        calc = CompositeSignalCalculator()
        calc.process_all()
    except Exception as e:
        print("[ERROR]", str(e))

if __name__ == "__main__":
    main()
