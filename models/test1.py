import json
from pathlib import Path

DATA_DIR = Path("D:/telescopic_modelling/data/quarterly_income_statements")

def find_share_keys():
    found_keys = set()

    for file in DATA_DIR.glob("*.json"):
        with open(file, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except Exception:
                continue

        stmt = data.get("quarterly_income_statement")
        if not isinstance(stmt, dict):
            continue  # Skip if missing or not a dictionary

        for key in stmt.keys():
            key_lower = key.lower()
            if "share" in key_lower or "outstanding" in key_lower:
                found_keys.add(key)

    return sorted(found_keys)

def main():
    keys = find_share_keys()
    print("🔍 Share-related keys found across files:")
    for k in keys:
        print(" -", k)

if __name__ == "__main__":
    main()
