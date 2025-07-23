import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Tuple
from collections import defaultdict
from tqdm import tqdm

sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection


REQUIRED_FIELDS = ["diluted_eps", "total_revenue"]

class DerivedMetricsPipeline:
    def __init__(self):
        self.db = DatabaseConnection()
        self.conn = self.db.connect()

    def backup_table(self):
        with self.conn.cursor() as cur:
            print("[INFO] Backing up derived_metrics table...")
            cur.execute("DROP TABLE IF EXISTS derived_metrics_backup;")
            cur.execute("CREATE TABLE derived_metrics_backup AS SELECT * FROM derived_metrics;")
            self.conn.commit()

    def fetch_annual_data(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        cur = self.conn.cursor()
        data = defaultdict(lambda: defaultdict(dict))

        cur.execute("""SELECT ticker, period_ending, diluted_eps, total_revenue, net_income, operating_income FROM income_statement_annual""")
        for row in cur.fetchall():
            t, pe, eps, rev, ni, op = row
            d = data[t][pe]
            d['diluted_eps'] = eps
            d['total_revenue'] = rev
            d['net_income'] = ni
            d['operating_income'] = op

        cur.execute("""SELECT ticker, period_ending, stockholders_equity, total_assets, total_debt, current_assets, current_liabilities FROM balance_sheet_annual""")
        for row in cur.fetchall():
            t, pe, se, ta, td, ca, cl = row
            d = data[t][pe]
            d['stockholders_equity'] = se
            d['total_assets'] = ta
            d['total_debt'] = td
            d['current_assets'] = ca
            d['current_liabilities'] = cl

        cur.execute("""SELECT ticker, period_ending, operating_cash_flow, free_cash_flow, dividends_paid FROM cash_flow_annual""")
        for row in cur.fetchall():
            t, pe, ocf, fcf, div = row
            d = data[t][pe]
            d['operating_cash_flow'] = ocf
            d['free_cash_flow'] = fcf
            d['dividends_paid'] = div

        cur.close()
        return data

    def compute_fiscal_year(self, pe: datetime) -> int:
        return pe.year - 1 if pe.month == 3 and pe.day == 31 else pe.year

    def recreate_table(self):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS derived_metrics;")
        cur.execute("""
            CREATE TABLE derived_metrics (
                ticker TEXT,
                fiscal_year INT,
                eps NUMERIC,
                revenue NUMERIC,
                net_income NUMERIC,
                operating_income NUMERIC,
                stockholders_equity NUMERIC,
                total_assets NUMERIC,
                total_debt NUMERIC,
                current_assets NUMERIC,
                current_liabilities NUMERIC,
                operating_cash_flow NUMERIC,
                free_cash_flow NUMERIC,
                dividends_paid NUMERIC,
                period_ending DATE,
                eps_cagr_2y NUMERIC,
                target_pe NUMERIC,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, fiscal_year)
            );
        """)
        self.conn.commit()
        cur.close()

    def insert_data(self, rows: List[Tuple]):
        cur = self.conn.cursor()
        cur.executemany("""
            INSERT INTO derived_metrics (
                ticker, fiscal_year,
                eps, revenue, net_income, operating_income,
                stockholders_equity, total_assets, total_debt,
                current_assets, current_liabilities,
                operating_cash_flow, free_cash_flow, dividends_paid,
                period_ending, last_updated
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, rows)
        self.conn.commit()
        cur.close()

    def build_metrics(self) -> int:
        data = self.fetch_annual_data()
        rows, skipped = [], []
        for ticker, recs in tqdm(data.items(), desc="Processing records"):
            for pe, vals in recs.items():
                if not all(vals.get(k) is not None for k in REQUIRED_FIELDS):
                    skipped.append((ticker, pe))
                    continue

                row = (
                    ticker,
                    self.compute_fiscal_year(pe),
                    vals.get("diluted_eps"),
                    vals.get("total_revenue"),
                    vals.get("net_income"),
                    vals.get("operating_income"),
                    vals.get("stockholders_equity"),
                    vals.get("total_assets"),
                    vals.get("total_debt"),
                    vals.get("current_assets"),
                    vals.get("current_liabilities"),
                    vals.get("operating_cash_flow"),
                    vals.get("free_cash_flow"),
                    vals.get("dividends_paid"),
                    pe
                )
                rows.append(row)

        with open("skipped_derived_metrics.log", "w") as f:
            for t, pe in skipped:
                f.write(f"{t},{pe}\n")

        self.recreate_table()
        self.insert_data(rows)
        print(f"[INFO] Inserted: {len(rows)}, Skipped: {len(skipped)}")
        return len(rows)

    def compute_eps_cagr(self):
        cur = self.conn.cursor()
        cur.execute("SELECT ticker, fiscal_year, eps FROM derived_metrics WHERE eps IS NOT NULL")
        rows = cur.fetchall()

        eps_map = defaultdict(list)
        for t, fy, eps in rows:
            eps_map[t].append((fy, eps))

        updates = []
        for t, vals in eps_map.items():
            vals.sort()
            for i in range(2, len(vals)):
                fy_now, eps_now = vals[i]
                fy_old, eps_old = vals[i - 2]
                try:
                    if eps_now is not None and eps_old is not None and eps_now > 0 and eps_old > 0:
                        cagr = (float(eps_now) / float(eps_old)) ** 0.5 - 1
                    else:
                        cagr = None
                    updates.append((cagr, t, fy_now))
                except Exception:
                    continue

        cur.executemany("""
            UPDATE derived_metrics
            SET eps_cagr_2y = %s, last_updated = CURRENT_TIMESTAMP
            WHERE ticker = %s AND fiscal_year = %s
        """, updates)
        self.conn.commit()
        cur.close()
        print(f"[INFO] EPS CAGR updated: {len(updates)}")

    def compute_target_pe(self):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (d.ticker, d.fiscal_year)
                d.ticker, d.fiscal_year, d.period_ending,
                v.ttm_eps, v.entry_price
            FROM derived_metrics d
            JOIN valuation_snapshots v ON d.ticker = v.ticker
            WHERE d.target_pe IS NULL
              AND v.ttm_eps > 0 AND v.entry_price > 0
              AND v.as_of_date BETWEEN d.period_ending AND d.period_ending + INTERVAL '1 year'
            ORDER BY d.ticker, d.fiscal_year, v.as_of_date
        """)
        rows = cur.fetchall()

        updates = []
        for t, fy, pe, eps, price in rows:
            try:
                updates.append((round(price / eps, 2), t, fy))
            except ZeroDivisionError:
                continue

        cur.executemany("""
            UPDATE derived_metrics
            SET target_pe = %s, last_updated = CURRENT_TIMESTAMP
            WHERE ticker = %s AND fiscal_year = %s
        """, updates)
        self.conn.commit()
        cur.close()
        print(f"[INFO] Target PE updated: {len(updates)}")

    def run(self):
        self.backup_table()
        count = self.build_metrics()
        if count > 0:
            self.compute_eps_cagr()
            self.compute_target_pe()
        print("[OK] Pipeline complete")


if __name__ == "__main__":
    pipeline = DerivedMetricsPipeline()
    pipeline.run()
