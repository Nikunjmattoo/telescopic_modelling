import os
import json
import pandas as pd
from datetime import datetime
from collections import defaultdict
import glob

def analyze_quarterly_coverage():
    """Analyze quarterly financial data coverage."""
    # Initialize data structures
    quarterly_coverage = defaultdict(lambda: {
        'total_companies': 0,
        'has_income_statement': 0,
        'has_balance_sheet': 0,
        'has_cash_flow': 0,
        'all_statements': 0,
        'quarters_available': defaultdict(int)
    })
    
    # Get all JSON files
    files = glob.glob('data/quarterly_financials/*.json')
    print(f"Found {len(files)} files to analyze...")
    
    for file_path in files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            if not data.get('data_available', False):
                continue
                
            ticker = data.get('ticker', '')
            
            # Check each statement type
            has_income = 'income_statement' in data and bool(data['income_statement'])
            has_balance = 'balance_sheet' in data and bool(data['balance_sheet'])
            has_cash = 'cash_flow' in data and bool(data['cash_flow'])
            
            # Get quarters from income statement (if available)
            if has_income:
                quarters = set()
                for metric in data['income_statement'].values():
                    for date_str in metric.keys():
                        try:
                            # Convert date to quarter
                            date = datetime.strptime(date_str.strip(), '%Y-%m-%d %H:%M:%S')
                            quarter = f"{date.year} Q{(date.month - 1) // 3 + 1}"
                            quarters.add(quarter)
                        except (ValueError, AttributeError):
                            continue
                
                # Update coverage for each quarter
                for quarter in quarters:
                    quarterly_coverage[quarter]['total_companies'] += 1
                    if has_income:
                        quarterly_coverage[quarter]['has_income_statement'] += 1
                    if has_balance:
                        quarterly_coverage[quarter]['has_balance_sheet'] += 1
                    if has_cash:
                        quarterly_coverage[quarter]['has_cash_flow'] += 1
                    if has_income and has_balance and has_cash:
                        quarterly_coverage[quarter]['all_statements'] += 1
                    
                    # Track number of quarters per company
                    quarterly_coverage[quarter]['quarters_available'][ticker] = len(quarters)
                        
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
    
    # Convert to DataFrame for better display
    df = pd.DataFrame.from_dict(quarterly_coverage, orient='index')
    
    # Calculate percentages
    for col in ['has_income_statement', 'has_balance_sheet', 'has_cash_flow', 'all_statements']:
        df[f'{col}_pct'] = (df[col] / df['total_companies'] * 100).round(2)
    
    # Sort by quarter (most recent first)
    df = df.sort_index(ascending=False)
    
    # Save to CSV
    os.makedirs('reports', exist_ok=True)
    report_file = 'reports/quarterly_financials_coverage.csv'
    df.to_csv(report_file, float_format='%.2f')
    
    # Calculate and print summary statistics
    total_companies = len(files)
    companies_with_data = df['total_companies'].max() if not df.empty else 0
    
    print(f"\nQuarterly Financials Coverage Report saved to {report_file}")
    print("\nSummary:")
    print(f"Total companies processed: {total_companies}")
    print(f"Companies with quarterly data: {companies_with_data} ({(companies_with_data/total_companies*100):.1f}%)")
    
    if not df.empty:
        print("\nCoverage by Quarter (most recent first):")
        print(df[['total_companies', 'all_statements_pct']].to_string())
        
        # Calculate average number of quarters per company
        all_quarters = []
        for quarters in df['quarters_available']:
            all_quarters.extend(quarters.values())
        
        if all_quarters:
            avg_quarters = sum(all_quarters) / len(all_quarters)
            print(f"\nAverage number of quarters per company: {avg_quarters:.1f}")
    
    return df

if __name__ == "__main__":
    analyze_quarterly_coverage()
