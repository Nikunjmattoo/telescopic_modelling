import sys
from pathlib import Path
from datetime import date, datetime

sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection


def get_quarter_ends(start_year=2015) -> list[date]:
    return [
        date(y, m, d)
        for y in range(start_year, datetime.today().year + 1)
        for (m, d) in [(3, 31), (6, 30), (9, 30), (12, 31)]
    ]


def audit_composite_input_coverage():
    db = DatabaseConnection()
    conn = db.connect()
    cur = conn.cursor()

    print(f"{'Quarter':<12} {'Valuation':>10} {'Momentum':>10} {'Fundamental':>12} {'Composite OK':>14}")
    print("-" * 64)

    for q in get_quarter_ends():
        # Valuation coverage
        cur.execute("""
            SELECT DISTINCT ticker
            FROM valuation_signals
            WHERE as_of_date = %s AND valuation_signal IS NOT NULL
        """, (q,))
        valuation = {row[0] for row in cur.fetchall()}

        # Momentum coverage
        cur.execute("""
            SELECT DISTINCT ticker
            FROM momentum_signals
            WHERE as_of_date = %s AND momentum_3m IS NOT NULL AND momentum_6m IS NOT NULL AND momentum_12m IS NOT NULL
        """, (q,))
        momentum = {row[0] for row in cur.fetchall()}

        # Fundamental coverage
        cur.execute("""
            SELECT DISTINCT ticker
            FROM fundamental_scores
            WHERE as_of_date = %s AND eps_growth IS NOT NULL AND revenue_growth IS NOT NULL
              AND roe IS NOT NULL AND net_margin IS NOT NULL
        """, (q,))
        fundamental = {row[0] for row in cur.fetchall()}

        # Intersection (tickers that have all signals)
        composite_ok = valuation & momentum & fundamental

        print(f"{q} {len(valuation):>10} {len(momentum):>10} {len(fundamental):>12} {len(composite_ok):>14}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    audit_composite_input_coverage()
