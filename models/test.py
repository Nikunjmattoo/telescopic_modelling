import os
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import math
from tqdm import tqdm

# Define constants
DATA_DIR = Path("D:/telescopic_modelling/data/quarterly_income_statements")
TARGET_FIELD = "Basic EPS"
QUARTER_DIFFS = [0, 1, 2, 3]

# ✅ Additional fields to check
REQUIRED_FIELDS = {
    "Net Income": "net_income_data",
    "Otherunder Preferred Stock Dividend": "preferred_dividends_data",
    "Basic Average Shares": "shares_basic_data",
    "Diluted Average Shares": "shares_diluted_data"
}

def parse_date(qstr):
    try:
        return datetime.strptime(qstr.split(" ")[0], "%Y-%m-%d").date()
    except Exception:
        return None

def previous_quarter(date, offset):
    q_months = [3, 6, 9, 12]
    current_q_index = (date.month - 1) // 3
    total_quarters = current_q_index - offset
    new_year = date.year + (total_quarters // 4)
    new_q_index = total_quarters % 4
    if new_q_index < 0:
        new_q_index += 4
        new_year -= 1
    month = q_months[new_q_index]
    day = {3: 31, 6: 30, 9: 30, 12: 31}[month]
    return datetime(new_year, month, day).date()

def extract_field_data(field_name):
    field_data = defaultdict(dict)
    json_files = list(DATA_DIR.glob("*.json"))
    for file in tqdm(json_files, desc=f"Scanning {field_name}"):
        ticker = file.stem.replace(".NS", "") + ".NS"
        with open(file, "r") as f:
            try:
                data = json.load(f)
            except Exception:
                continue
        statement = data.get("quarterly_income_statement")
        if not statement or not isinstance(statement, dict):
            continue
        block = statement.get(field_name)
        if not block or not isinstance(block, dict):
            continue
        for date_str, val in block.items():
            date_obj = parse_date(date_str)
            if not date_obj:
                continue
            if isinstance(val, (int, float)) and not math.isnan(val):
                field_data[ticker][date_obj] = val
    return field_data

def get_all_data():
    data = {
        "eps_data": extract_field_data("Basic EPS")
    }
    for json_field, internal_key in REQUIRED_FIELDS.items():
        data[internal_key] = extract_field_data(json_field)
    return data

def build_summary(all_data):
    eps_data = all_data["eps_data"]
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
            "all_4_available": 0,
            "current_net_income_count": 0,
            "current_preferred_dividends_count": 0,
            "current_shares_outstanding_count": 0  # ✅ either basic or diluted
        }

        for ticker in eps_data:
            if q in eps_data[ticker]:
                row["current_eps_count"] += 1
                has_all = True
                for i, col in zip([1, 2, 3], ["prev_1q_count", "prev_2q_count", "prev_3q_count"]):
                    q_i = previous_quarter(q, i)
                    if q_i in eps_data[ticker]:
                        row[col] += 1
                    else:
                        has_all = False
                if has_all:
                    row["all_4_available"] += 1

            # ✅ Additional fields
            if q in all_data["net_income_data"].get(ticker, {}):
                row["current_net_income_count"] += 1
            if q in all_data["preferred_dividends_data"].get(ticker, {}):
                row["current_preferred_dividends_count"] += 1

            # ✅ Either basic or diluted shares count as available
            has_basic = q in all_data["shares_basic_data"].get(ticker, {})
            has_diluted = q in all_data["shares_diluted_data"].get(ticker, {})
            if has_basic or has_diluted:
                row["current_shares_outstanding_count"] += 1

        results.append(row)
    return results

def main():
    all_data = get_all_data()
    summary = build_summary(all_data)

    # Output as TSV table (readable in console or copy-paste to Excel)
    headers = [
        "current_quarter", "current_eps_count",
        "prev_1q_count", "prev_2q_count", "prev_3q_count", "all_4_available",
        "current_net_income_count", "current_preferred_dividends_count", "current_shares_outstanding_count"
    ]
    print("\t".join(headers))
    for row in summary:
        print("\t".join(str(row[h]) if not isinstance(row[h], datetime) else row[h].isoformat() for h in headers))

if __name__ == "__main__":
    main()
