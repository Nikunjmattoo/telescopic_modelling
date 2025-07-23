import sys
from pathlib import Path
from datetime import date

sys.path.append(str(Path(__file__).parent.parent))  # Fix path to access db_utils

from db_utils import DatabaseConnection

db = DatabaseConnection()
conn = db.connect()
cur = conn.cursor()

cur.execute("""
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
""", (
    'TEST_TICKER', date(2023, 12, 31), 10.0, True, 200.0, 20.0, date(2023, 12, 31)
))

conn.commit()
print("[OK] Test insert complete.")
