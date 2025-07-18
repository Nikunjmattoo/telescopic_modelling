import json
import os
from collections import defaultdict

def analyze_balance_sheet_fields():
    """Analyze all JSON files to find field variations"""
    
    balance_sheet_dir = r"d:\telescopic_modelling\data\balance_sheets"
    all_fields = set()
    field_frequency = defaultdict(int)
    files_analyzed = 0
    
    # Target fields we're looking for variations of
    target_patterns = {
        'assets': ['asset', 'total asset'],
        'liabilities': ['liabilit', 'total liabilit'],
        'current_assets': ['current asset'],
        'current_liabilities': ['current liabilit'],
        'stockholders_equity': ['stockholder', 'equity', 'shareholder'],
        'total_debt': ['debt', 'total debt']
    }
    
    categorized_fields = {pattern: [] for pattern in target_patterns.keys()}
    
    # Analyze first 30 files
    json_files = [f for f in os.listdir(balance_sheet_dir) if f.endswith('.json')][:30]
    
    for filename in json_files:
        try:
            filepath = os.path.join(balance_sheet_dir, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'annual_balance_sheet' in data:
                balance_sheet = data['annual_balance_sheet']
                for field in balance_sheet.keys():
                    all_fields.add(field)
                    field_frequency[field] += 1
                    
                    # Categorize fields based on patterns
                    field_lower = field.lower()
                    for pattern, keywords in target_patterns.items():
                        if any(keyword in field_lower for keyword in keywords):
                            categorized_fields[pattern].append(field)
            
            files_analyzed += 1
            print(f"Analyzed {filename}")
            
        except Exception as e:
            print(f"Error analyzing {filename}: {e}")
    
    print(f"\n=== ANALYSIS COMPLETE ===")
    print(f"Files analyzed: {files_analyzed}")
    print(f"Total unique fields found: {len(all_fields)}")
    
    print(f"\n=== FIELD CATEGORIZATION ===")
    for category, fields in categorized_fields.items():
        print(f"\n{category.upper()}:")
        unique_fields = list(set(fields))
        for field in unique_fields:
            freq = field_frequency[field]
            print(f"  - {field} (appears in {freq} files)")
    
    print(f"\n=== ALL FIELDS (sorted by frequency) ===")
    sorted_fields = sorted(field_frequency.items(), key=lambda x: x[1], reverse=True)
    for field, freq in sorted_fields:
        print(f"{freq:2d}x: {field}")

if __name__ == "__main__":
    analyze_balance_sheet_fields()
