#!/usr/bin/env python3

import json
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import math

sys.path.append(str(Path(__file__).parent.parent))
from db_utils import DatabaseConnection

# Fields mapping
FIELD_MAPPINGS = {
    'total_revenue': ['Total Revenue'],
    'operating_income': ['Operating Income'],
    'net_income': ['Net Income'],
    'basic_eps': ['Earnings Per Share', 'EPS - Basic', 'EPS - Basic (Rs.)'],
    'diluted_eps': ['EPS - Diluted', 'Diluted EPS']
}

def get_existing_keys(cursor):
    cursor.execute("SELECT ticker, period_ending FROM income_statement_quarterly")
    return set(cursor.fetchall())

def extract_field(data, date_str, possible_keys):
    for key in possible_keys:
        if key in data and isinstance(data[key], dict) and date_str in data[key]:
            val = data[key][date_str]
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                return val
    return None

def process_json_file(file_path, ticker):
    with open(file_path, "r") as f:
        data = json.load(f)

    if "quarterly_income_statement" not in data:
        return []

    qdata = data["quarterly_income_statement"]
    all_dates = set()
    for values in qdata.values():
        if isinstance(values, dict):
            all_dates.update(values.keys())

    valid_dates = []
    for ds in all_dates:
        try:
            dt = datetime.strptime(ds.split(" ")[0], "%Y-%m-%d").date()
            if dt.month in [3, 6, 9, 12] and dt.day in [31, 30]:
                valid_dates.append((ds, dt))
        except:
            continue

    records = []
    for date_str, date_obj in valid_dates:
        row = {
            "ticker": ticker,
            "period_ending": date_obj,
            "total_revenue": extract_field(qdata, date_str, FIELD_MAPPINGS['total_revenue']),
            "operating_income": extract_field(qdata, date_str, FIELD_MAPPINGS['operating_income']),
            "net_income": extract_field(qdata, date_str, FIELD_MAPPINGS['net_income']),
            "basic_eps": extract_field(qdata, date_str, FIELD_MAPPINGS['basic_eps']),
            "diluted_eps": extract_field(qdata, date_str, FIELD_MAPPINGS['diluted_eps']),
            "last_updated": datetime.now()
        }

        if any(row[f] is not None for f in FIELD_MAPPINGS):
            records.append(row)
    return records

def main():
    db = DatabaseConnection()
    conn = db.connect()
    cur = conn.cursor()

    data_dir = Path(__file__).parent.parent / "data" / "quarterly_income_statements"
    json_files = list(data_dir.glob("*.json"))

    print(f"Scanning {len(json_files)} JSON files in {data_dir}...")
    existing_keys = get_existing_keys(cur)

    insert_count, update_count = 0, 0

    for file in tqdm(json_files, desc="Processing"):
        ticker = file.stem.replace('.NS', '') + '.NS' if '.NS' in file.stem else file.stem
        records = process_json_file(file, ticker)

        for row in records:
            key = (row['ticker'], row['period_ending'])

            cur.execute("""
                SELECT total_revenue, operating_income, net_income, basic_eps, diluted_eps
                FROM income_statement_quarterly
                WHERE ticker = %s AND period_ending = %s
            """, key)
            existing = cur.fetchone()

            if existing:
                existing_dict = dict(zip(['total_revenue', 'operating_income', 'net_income', 'basic_eps', 'diluted_eps'], existing))
                needs_update = any(row[k] != existing_dict[k] for k in existing_dict)
                if needs_update:
                    cur.execute("""
                        UPDATE income_statement_quarterly
                        SET total_revenue = %s, operating_income = %s, net_income = %s,
                            basic_eps = %s, diluted_eps = %s, last_updated = %s
                        WHERE ticker = %s AND period_ending = %s
                    """, (
                        row['total_revenue'], row['operating_income'], row['net_income'],
                        row['basic_eps'], row['diluted_eps'], row['last_updated'],
                        row['ticker'], row['period_ending']
                    ))
                    update_count += 1
            else:
                cur.execute("""
                    INSERT INTO income_statement_quarterly (
                        ticker, period_ending, total_revenue, operating_income,
                        net_income, basic_eps, diluted_eps, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['ticker'], row['period_ending'], row['total_revenue'],
                    row['operating_income'], row['net_income'],
                    row['basic_eps'], row['diluted_eps'], row['last_updated']
                ))
                insert_count += 1

    conn.commit()
    cur.close()
    conn.close()

    print("\n✅ DONE")
    print(f"Inserted: {insert_count}")
    print(f"Updated:  {update_count}")

if __name__ == "__main__":
    main()
