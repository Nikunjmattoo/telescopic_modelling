import json
from pathlib import Path

def analyze_cashflow_fields():
    data_dir = Path("data/quarterly_cashflow")
    total_files = 0
    operating_cash_flow_count = 0
    cash_flow_from_ops_count = 0
    other_variations = set()
    
    # Count total JSON files
    json_files = list(data_dir.glob("*.json"))
    total_files = len(json_files)
    
    if total_files == 0:
        print("No JSON files found in the directory.")
        return
    
    # Analyze each file
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if not isinstance(data, dict):
                continue
                
            quarterly_data = data.get('quarterly_cashflow', {})
            if not quarterly_data:
                continue
                
            # Check for different variations of operating cash flow
            if "Operating Cash Flow" in quarterly_data:
                operating_cash_flow_count += 1
            elif "Cash Flow from Operating Activities" in quarterly_data:
                cash_flow_from_ops_count += 1
            else:
                # Look for other possible variations
                for key in quarterly_data.keys():
                    if any(term in key.lower() for term in ['operating', 'cash flow', 'ops', 'cf', 'ocf']):
                        if key not in ["Operating Cash Flow", "Cash Flow from Operating Activities"]:
                            other_variations.add(key)
                            
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"Error processing {json_file.name}: {str(e)}")
            continue
    
    # Print results
    print(f"\n=== CASH FLOW FIELD ANALYSIS ===")
    print(f"Total files analyzed: {total_files}")
    print(f"\nExact match 'Operating Cash Flow': {operating_cash_flow_count} files ({operating_cash_flow_count/total_files:.1%})")
    print(f"Exact match 'Cash Flow from Operating Activities': {cash_flow_from_ops_count} files ({cash_flow_from_ops_count/total_files:.1%})")
    
    if other_variations:
        print("\nOther potential operating cash flow fields found:")
        for field in sorted(other_variations):
            print(f"- {field}")
    
    total_ops_cash_flow = operating_cash_flow_count + cash_flow_from_ops_count
    print(f"\nTOTAL FILES WITH OPERATING CASH FLOW DATA: {total_ops_cash_flow} files ({total_ops_cash_flow/total_files:.1%})")

if __name__ == "__main__":
    analyze_cashflow_fields()
