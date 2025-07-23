import sys
from pathlib import Path
import pandas as pd

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from db_utils import DatabaseConnection

def main():
    db = DatabaseConnection()
    conn = db.connect()

    df = pd.read_sql("""
        SELECT as_of_date, holding_days, ticker, composite_score, return_value
        FROM composite_signal_backtest
        WHERE composite_score IS NOT NULL AND return_value IS NOT NULL
        ORDER BY as_of_date, holding_days, composite_score DESC
    """, conn)

    conn.close()

    df['return_value'] = (df['return_value'] * 100).round(2)  # Convert to %
    df['composite_score'] = df['composite_score'].round(4)

    print(df.head(30))  # Show first 30 rows in console

    # Save to CSV for full inspection
    df.to_csv("composite_vs_return.csv", index=False)
    print("\n[INFO] Saved full table to 'composite_vs_return.csv'.")

if __name__ == "__main__":
    main()
