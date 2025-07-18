import os
import json
import pandas as pd
from datetime import datetime
from collections import defaultdict
import glob

def analyze_balance_sheet_coverage():
    """Analyze balance sheet data coverage."""
    # Initialize data structures
    annual_coverage = defaultdict(lambda: {
        'total_companies': 0,
        'has_balance_sheet': 0,
        'has_quarterly_balance_sheet': 0,
        'has_both': 0,
        'key_metrics': defaultdict(int)
    })
    
    quarterly_coverage = defaultdict(lambda: {
        'total_companies': 0,
        'has_balance_sheet': 0
    })
    
    # Key balance sheet metrics to check
    key_metrics = [
        'Total Assets', 'Total Liabilities', 'Total Equity', 
        'Current Assets', 'Current Liabilities', 'Total Debt'
    ]
    
    # Get all JSON files
    files = glob.glob('data/balance_sheets/*.json')
    print(f"Found {len(files)} balance sheet files to analyze...")
    
    for file_path in files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            if not data.get('data_available', False):
                continue
                
            ticker = data.get('ticker', '')
            
            # Check for annual balance sheet
            has_annual = 'annual_balance_sheet' in data and bool(data['annual_balance_sheet'])
            has_quarterly = 'quarterly_balance_sheet' in data and bool(data['quarterly_balance_sheet'])
            
            # Process annual data
            if has_annual:
                annual_data = data['annual_balance_sheet']
                
                # Check key metrics
                metrics_found = []
                for metric in key_metrics:
                    if metric in annual_data:
                        metrics_found.append(metric)
                
                # Process each year
                for metric_data in annual_data.values():
                    for date_str in metric_data.keys():
                        try:
                            date = datetime.strptime(date_str.strip(), '%Y-%m-%d %H:%M:%S')
                            year = date.year
                            
                            annual_coverage[year]['total_companies'] += 1
                            annual_coverage[year]['has_balance_sheet'] += 1
                            if has_quarterly:
                                annual_coverage[year]['has_quarterly_balance_sheet'] += 1
                            if has_annual and has_quarterly:
                                annual_coverage[year]['has_both'] += 1
                            
                            # Track key metrics
                            for metric in metrics_found:
                                annual_coverage[year]['key_metrics'][metric] += 1
                                
                        except (ValueError, AttributeError):
                            continue
            
            # Process quarterly data
            if has_quarterly:
                quarterly_data = data['quarterly_balance_sheet']
                
                for metric_data in quarterly_data.values():
                    for date_str in metric_data.keys():
                        try:
                            date = datetime.strptime(date_str.strip(), '%Y-%m-%d %H:%M:%S')
                            quarter = f"{date.year} Q{(date.month - 1) // 3 + 1}"
                            
                            quarterly_coverage[quarter]['total_companies'] += 1
                            quarterly_coverage[quarter]['has_balance_sheet'] += 1
                                
                        except (ValueError, AttributeError):
                            continue
                                
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
    
    # Convert to DataFrames
    annual_df = pd.DataFrame.from_dict(annual_coverage, orient='index')
    quarterly_df = pd.DataFrame.from_dict(quarterly_coverage, orient='index')
    
    # Calculate percentages
    for df in [annual_df, quarterly_df]:
        if not df.empty:
            df['coverage_pct'] = (df['has_balance_sheet'] / df['total_companies'] * 100).round(2)
    
    # Sort indices
    annual_df = annual_df.sort_index(ascending=False)
    quarterly_df = quarterly_df.sort_index(ascending=False)
    
    # Save to CSV
    os.makedirs('reports', exist_ok=True)
    annual_file = 'reports/annual_balance_sheet_coverage.csv'
    quarterly_file = 'reports/quarterly_balance_sheet_coverage.csv'
    
    annual_df.to_csv(annual_file, float_format='%.2f')
    quarterly_df.to_csv(quarterly_file, float_format='%.2f')
    
    # Print summary
    print(f"\nAnnual Balance Sheet Coverage Report saved to {annual_file}")
    print(f"Quarterly Balance Sheet Coverage Report saved to {quarterly_file}")
    
    print("\nAnnual Coverage Summary:")
    print(annual_df[['total_companies', 'has_balance_sheet', 'coverage_pct']].to_string())
    
    if not quarterly_df.empty:
        print("\nQuarterly Coverage Summary (most recent first):")
        print(quarterly_df[['total_companies', 'has_balance_sheet', 'coverage_pct']].head(12).to_string())
    
    # Key metrics analysis
    if not annual_df.empty:
        print("\nKey Metrics Availability (latest year):")
        latest_year = annual_df.index.max()
        metrics_data = []
        for metric in key_metrics:
            count = annual_coverage[latest_year]['key_metrics'].get(metric, 0)
            total = annual_coverage[latest_year]['total_companies']
            pct = (count / total * 100) if total > 0 else 0
            metrics_data.append({
                'Metric': metric,
                'Companies': count,
                'Percentage': f"{pct:.1f}%"
            })
        
        metrics_df = pd.DataFrame(metrics_data)
        print(metrics_df.to_string(index=False))
    
    return annual_df, quarterly_df

if __name__ == "__main__":
    analyze_balance_sheet_coverage()
