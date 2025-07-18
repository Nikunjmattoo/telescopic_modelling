import os
import json
import pandas as pd
from datetime import datetime
from collections import defaultdict
import glob

def analyze_quarterly_coverage():
    """Analyze quarterly coverage of financial data."""
    # Initialize data structures
    quarterly_coverage = defaultdict(lambda: {
        'total_companies': 0,
        'has_income_statement': 0,
        'has_balance_sheet': 0,
        'has_cash_flow': 0,
        'all_statements': 0
    })
    
    # Get all JSON files
    files = glob.glob('data/financials/*.json')
    
    for file_path in files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                
            ticker = data.get('ticker', '')
            
            # Check each statement type
            has_income = 'income_statement' in data and bool(data['income_statement'])
            has_balance = 'balance_sheet' in data and bool(data['balance_sheet'])
            has_cash = 'cash_flow' in data and bool(data['cash_flow'])
            
            # Get quarters from income statement (if available)
            if has_income:
                for date_str in data['income_statement'].get('EBITDA', {}).keys():
                    try:
                        # Convert date to quarter
                        date = datetime.strptime(date_str.strip(), '%Y-%m-%d %H:%M:%S')
                        quarter = f"{date.year} Q{(date.month - 1) // 3 + 1}"
                        
                        # Update coverage
                        quarterly_coverage[quarter]['total_companies'] += 1
                        if has_income:
                            quarterly_coverage[quarter]['has_income_statement'] += 1
                        if has_balance:
                            quarterly_coverage[quarter]['has_balance_sheet'] += 1
                        if has_cash:
                            quarterly_coverage[quarter]['has_cash_flow'] += 1
                        if has_income and has_balance and has_cash:
                            quarterly_coverage[quarter]['all_statements'] += 1
                    except (ValueError, AttributeError):
                        continue
                        
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
    
    # Convert to DataFrame for better display
    df = pd.DataFrame.from_dict(quarterly_coverage, orient='index')
    
    # Calculate percentages
    for col in ['has_income_statement', 'has_balance_sheet', 'has_cash_flow', 'all_statements']:
        df[f'{col}_pct'] = (df[col] / df['total_companies'] * 100).round(2)
    
    # Sort by quarter
    df = df.sort_index()
    
    # Save to CSV
    os.makedirs('reports', exist_ok=True)
    report_file = 'reports/financials_quarterly_coverage.csv'
    df.to_csv(report_file, float_format='%.2f')
    
    print(f"\nQuarterly Coverage Report saved to {report_file}")
    print("\nSummary:")
    print(df[['total_companies', 'all_statements_pct']].to_string())
    
    return df

if __name__ == "__main__":
    analyze_quarterly_coverage()
