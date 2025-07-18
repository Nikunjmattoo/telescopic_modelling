
import sys
from pathlib import Path
from datetime import datetime, timedelta, date
from typing import List, Optional, Tuple
from tqdm import tqdm

sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection


class ValuationSignalCalculator:
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
            cur.execute("SELECT DISTINCT ticker FROM valuation_snapshots ORDER BY ticker")
            return [row[0] for row in cur.fetchall()]

    def get_valuation_inputs(self, ticker: str, as_of_date: date):
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT vs.ttm_pe, vs.ttm_eps, dm.target_pe, dm.eps_cagr_2y
                FROM valuation_snapshots vs
                JOIN derived_metrics dm ON vs.ticker = dm.ticker AND vs.as_of_date = dm.period_ending
                WHERE vs.ticker = %s AND vs.as_of_date = %s
            """, (ticker, as_of_date))
            return cur.fetchone()

    def get_avg_pe_1y(self, ticker: str, as_of_date: date) -> Optional[float]:
        one_year_ago = as_of_date - timedelta(days=365)
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT AVG(ttm_pe)
                FROM valuation_snapshots
                WHERE ticker = %s AND as_of_date BETWEEN %s AND %s AND ttm_pe IS NOT NULL
            """, (ticker, one_year_ago, as_of_date))
            row = cur.fetchone()
            return row[0] if row else None

    def save_signals(self, data: List[Tuple]):
        if not data:
            return
        with self.connection.cursor() as cur:
            cur.executemany("""
                INSERT INTO valuation_signals (
                    ticker, as_of_date, ttm_pe, target_pe,
                    valuation_signal, peg_ratio, avg_pe_1y, undervalued_flag
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, as_of_date) DO UPDATE SET
                    ttm_pe = EXCLUDED.ttm_pe,
                    target_pe = EXCLUDED.target_pe,
                    valuation_signal = EXCLUDED.valuation_signal,
                    peg_ratio = EXCLUDED.peg_ratio,
                    avg_pe_1y = EXCLUDED.avg_pe_1y,
                    undervalued_flag = EXCLUDED.undervalued_flag
            """, data)
            self.connection.commit()

    def process_ticker(self, ticker: str) -> int:
        rows = []
        for qend in self.quarter_ends:
            inputs = self.get_valuation_inputs(ticker, qend)
            if not inputs:
                continue
            ttm_pe, ttm_eps, target_pe, eps_cagr = inputs
            if None in [ttm_pe, target_pe]:
                continue
            valuation_signal = (ttm_pe / target_pe) - 1
            avg_pe_1y = self.get_avg_pe_1y(ticker, qend)
            peg_ratio = (ttm_pe / eps_cagr) if eps_cagr and eps_cagr != 0 else None
            undervalued_flag = (valuation_signal < -0.2)

            row = (
                ticker,
                qend,
                round(ttm_pe, 2),
                round(target_pe, 2),
                round(valuation_signal, 4),
                round(peg_ratio, 4) if peg_ratio else None,
                round(avg_pe_1y, 2) if avg_pe_1y else None,
                undervalued_flag
            )
            rows.append(row)

        self.save_signals(rows)
        return len(rows)

    def process_all(self):
        tickers = self.get_tickers()
        total = 0
        for ticker in tqdm(tickers, desc="Processing valuation signals"):
            try:
                count = self.process_ticker(ticker)
                total += count
            except Exception as e:
                print(f"Error processing {ticker}: {e}")
        print(f"[OK] Completed. Total valuation signals saved: {total}")


def main():
    try:
        calc = ValuationSignalCalculator()
        calc.process_all()
    except Exception as e:
        print("[ERROR]", str(e))


if __name__ == "__main__":
    main()
