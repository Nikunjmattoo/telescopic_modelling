import sys
from pathlib import Path
from datetime import datetime, date
from typing import List, Optional, Tuple
from tqdm import tqdm

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection  # type: ignore


class FundamentalScoreCalculator:
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

    def get_tickers(self) -> List[str]:
        with self.connection.cursor() as cur:
            cur.execute("SELECT DISTINCT ticker FROM income_statement_quarterly ORDER BY ticker")
            return [row[0] for row in cur.fetchall()]

    def get_quarter_value(self, table: str, column: str, ticker: str, period: date) -> Optional[float]:
        with self.connection.cursor() as cur:
            cur.execute(f"""
                SELECT {column}
                FROM {table}
                WHERE ticker = %s AND period_ending <= %s AND {column} IS NOT NULL
                ORDER BY period_ending DESC
                LIMIT 1
            """, (ticker, period))
            row = cur.fetchone()
            return row[0] if row else None

    def get_previous_quarter_value(self, table: str, column: str, ticker: str, period: date) -> Optional[float]:
        with self.connection.cursor() as cur:
            cur.execute(f"""
                SELECT {column}
                FROM {table}
                WHERE ticker = %s AND period_ending < %s AND {column} IS NOT NULL
                ORDER BY period_ending DESC
                LIMIT 1
            """, (ticker, period))
            row = cur.fetchone()
            return row[0] if row else None

    def save_scores(self, data: List[Tuple]):
        if not data:
            return
        with self.connection.cursor() as cur:
            cur.executemany("""
                INSERT INTO fundamental_scores (
                    ticker, period_ending, as_of_date,
                    eps_growth, revenue_growth, roe,
                    debt_to_equity, net_margin, fcf_margin
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, as_of_date) DO UPDATE SET
                    eps_growth = EXCLUDED.eps_growth,
                    revenue_growth = EXCLUDED.revenue_growth,
                    roe = EXCLUDED.roe,
                    debt_to_equity = EXCLUDED.debt_to_equity,
                    net_margin = EXCLUDED.net_margin,
                    fcf_margin = EXCLUDED.fcf_margin
            """, data)
            self.connection.commit()

    def process_all(self):
        tickers = self.get_tickers()
        total = 0
        skipped = 0
        for ticker in tqdm(tickers, desc="Processing fundamental scores"):
            rows = []
            for qend in self.quarter_ends:
                eps = self.get_quarter_value("income_statement_quarterly", "basic_eps", ticker, qend)
                eps_prev = self.get_previous_quarter_value("income_statement_quarterly", "basic_eps", ticker, qend)
                revenue = self.get_quarter_value("income_statement_quarterly", "total_revenue", ticker, qend)
                revenue_prev = self.get_previous_quarter_value("income_statement_quarterly", "total_revenue", ticker, qend)
                net_income = self.get_quarter_value("income_statement_quarterly", "net_income", ticker, qend)
                equity = self.get_quarter_value("balance_sheet_quarterly", "stockholders_equity", ticker, qend)
                total_debt = self.get_quarter_value("balance_sheet_quarterly", "total_debt", ticker, qend)
                fcf = self.get_quarter_value("cash_flow_quarterly", "free_cash_flow", ticker, qend)

                # Skip if anything critical is missing or if zero division risk
                if None in [eps, eps_prev, revenue, revenue_prev, net_income, equity, total_debt, fcf]:
                    skipped += 1
                    continue
                if equity == 0 or revenue == 0:
                    skipped += 1
                    continue

                try:
                    eps_growth = (eps - eps_prev) / abs(eps_prev) if eps_prev != 0 else None
                    revenue_growth = (revenue - revenue_prev) / abs(revenue_prev) if revenue_prev != 0 else None
                    roe = net_income / equity
                    debt_to_equity = total_debt / equity
                    net_margin = net_income / revenue
                    fcf_margin = fcf / revenue

                    row = (
                        ticker,
                        qend,  # period_ending
                        qend,  # as_of_date
                        round(eps_growth, 4),
                        round(revenue_growth, 4),
                        round(roe, 4),
                        round(debt_to_equity, 4),
                        round(net_margin, 4),
                        round(fcf_margin, 4)
                    )
                    rows.append(row)
                except Exception:
                    skipped += 1
                    continue

            self.save_scores(rows)
            total += len(rows)

        print(f"[SUMMARY] Fundamental Score Stats:")
        print(f"Total saved: {total}")
        print(f"Skipped rows (due to missing or invalid data): {skipped}")


def main():
    try:
        calc = FundamentalScoreCalculator()
        calc.process_all()
    except Exception as e:
        print("[ERROR]", str(e))


if __name__ == "__main__":
    main()
