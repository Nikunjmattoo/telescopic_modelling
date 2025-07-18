import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
import math
from tqdm import tqdm

# Define constants
DATA_DIR = Path("D:/telescopic_modelling/data/quarterly_income_statements")
TARGET_FIELD = "Basic EPS"
QUARTER_DIFFS = [0, 1, 2, 3]

def parse_date(qstr):
    try:
        return datetime.strptime(qstr.split(" ")[0], "%Y-%m-%d").date()
    except Exception:
        return None

def previous_quarter(date, offset):
    # Determine the quarter number and year
    q_months = [3, 6, 9, 12]
    current_q_index = (date.month - 1) // 3  # 0 for Q1, ..., 3 for Q4
    total_quarters = current_q_index - offset

    # Calculate year and quarter after applying offset
    new_year = date.year + (total_quarters // 4)
    new_q_index = total_quarters % 4

    if new_q_index < 0:
        new_q_index += 4
        new_year -= 1

    month = q_months[new_q_index]
    day = {3: 31, 6: 30, 9: 30, 12: 31}[month]

    return datetime(new_year, month, day).date()

def get_eps_by_ticker():
    eps_data = defaultdict(dict)  # ticker -> {date: value}

    json_files = list(DATA_DIR.glob("*.json"))
    for file in tqdm(json_files, desc="Scanning JSON files"):
        ticker = file.stem.replace(".NS", "") + ".NS"  # Normalize ticker
        with open(file, "r") as f:
            try:
                data = json.load(f)
            except Exception:
                continue

        statement = data.get("quarterly_income_statement")
        if not statement or not isinstance(statement, dict):
            continue  # Skip if missing or invalid

        eps_block = statement.get(TARGET_FIELD)
        if not eps_block or not isinstance(eps_block, dict):
            continue  # Skip if Basic EPS is missing

        for date_str, val in eps_block.items():
            date_obj = parse_date(date_str)
            if not date_obj:
                continue
            if isinstance(val, (int, float)) and not math.isnan(val):
                eps_data[ticker][date_obj] = val
    return eps_data

def build_summary(eps_data):
    quarter_set = set()
    for ticker_dates in eps_data.values():
        quarter_set.update(ticker_dates.keys())
    quarter_list = sorted(quarter_set)

    results = []
    for q in quarter_list:
        row = {
            "current_quarter": q,
            "current_eps_count": 0,
            "prev_1q_count": 0,
            "prev_2q_count": 0,
            "prev_3q_count": 0,
            "all_4_available": 0
        }
        for ticker, dates in eps_data.items():
            if q in dates:
                row["current_eps_count"] += 1
                has_all = True
                for i, col in zip([1, 2, 3], ["prev_1q_count", "prev_2q_count", "prev_3q_count"]):
                    q_i = previous_quarter(q, i)
                    if q_i in dates:
                        row[col] += 1
                    else:
                        has_all = False
                if has_all:
                    row["all_4_available"] += 1
        results.append(row)
    return results

def main():
    eps_data = get_eps_by_ticker()
    summary = build_summary(eps_data)

    # Output as CSV-style table (with headers)
    print("current_quarter\tcurrent_eps_count\tprev_1q_count\tprev_2q_count\tprev_3q_count\tall_4_available")
    for row in summary:
        print(f"{row['current_quarter'].isoformat()}\t{row['current_eps_count']}\t{row['prev_1q_count']}\t{row['prev_2q_count']}\t{row['prev_3q_count']}\t{row['all_4_available']}")

if __name__ == "__main__":
    main()
