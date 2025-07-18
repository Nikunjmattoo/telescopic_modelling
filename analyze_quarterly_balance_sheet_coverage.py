import os
import json
import pandas as pd
from datetime import datetime
from collections import defaultdict
import glob

def analyze_quarterly_balance_sheet_coverage():
    """Analyze quarterly balance sheet data coverage."""
    # Initialize data structures
    quarterly_coverage = defaultdict(lambda: {
        'total_companies': 0,
        'has_balance_sheet': 0,
        'key_metrics': defaultdict(int)
    })
    
    # Key balance sheet metrics to check
    key_metrics = [
        'Total Assets', 'Total Liabilities', 'Total Equity', 
        'Current Assets', 'Current Liabilities', 'Total Debt',
        'Cash And Cash Equivalents', 'Total Current Assets',
        'Net Debt', 'Total Non Current Assets',
        'Property Plant Equipment Net', 'Total Non Current Liabilities'
    ]
    
    # Get all JSON files
    files = glob.glob('data/quarterly_balance_sheets/*.json')
    print(f"Found {len(files)} quarterly balance sheet files to analyze...")
    
    for file_path in files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            if not data.get('data_available', False):
                continue
                
            ticker = data.get('ticker', '')
            
            # Check for quarterly balance sheet data
            if 'quarterly_balance_sheet' in data and data['quarterly_balance_sheet']:
                qbs = data['quarterly_balance_sheet']
                
                # Check key metrics
                metrics_found = []
                for metric in key_metrics:
                    if metric in qbs:
                        metrics_found.append(metric)
                
                # Process each quarter
                for metric_data in qbs.values():
                    for date_str in metric_data.keys():
                        try:
                            date = datetime.strptime(date_str.strip(), '%Y-%m-%d %H:%M:%S')
                            quarter = f"{date.year} Q{(date.month - 1) // 3 + 1}"
                            
                            quarterly_coverage[quarter]['total_companies'] += 1
                            quarterly_coverage[quarter]['has_balance_sheet'] += 1
                            
                            # Track key metrics
                            for metric in metrics_found:
                                quarterly_coverage[quarter]['key_metrics'][metric] += 1
                                
                        except (ValueError, AttributeError):
                            continue
                                
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
    
    # Convert to DataFrame for better display
    df = pd.DataFrame.from_dict(quarterly_coverage, orient='index')
    
    # Calculate coverage percentages
    if not df.empty:
        df['coverage_pct'] = (df['has_balance_sheet'] / df['total_companies'] * 100).round(2)
        
        # Calculate metrics coverage
        for metric in key_metrics:
            df[f"{metric}_pct"] = (df['key_metrics'].apply(lambda x: x.get(metric, 0)) / 
                                  df['total_companies'] * 100).round(2)
    
    # Sort by quarter (most recent first)
    df = df.sort_index(ascending=False)
    
    # Save to CSV
    os.makedirs('reports', exist_ok=True)
    report_file = 'reports/quarterly_balance_sheet_coverage_detailed.csv'
    df.to_csv(report_file, float_format='%.2f')
    
    # Print summary
    print(f"\nQuarterly Balance Sheet Coverage Report saved to {report_file}")
    print("\nSummary by Quarter (most recent first):")
    
    if not df.empty:
        # Show key columns
        summary_cols = ['total_companies', 'has_balance_sheet', 'coverage_pct']
        for metric in key_metrics:
            if f"{metric}_pct" in df.columns:
                summary_cols.append(f"{metric}_pct")
        
        print(df[summary_cols].head(12).to_string())
        
        # Calculate overall metrics coverage
        print("\nOverall Metrics Coverage (across all quarters):")
        metrics_coverage = {}
        for metric in key_metrics:
            if f"{metric}_pct" in df.columns:
                metrics_coverage[metric] = df[f"{metric}_pct"].mean().round(2)
        
        metrics_df = pd.DataFrame(list(metrics_coverage.items()), 
                                columns=['Metric', 'Average Coverage %'])
        print(metrics_df.sort_values('Average Coverage %', ascending=False).to_string(index=False))
    
    return df

if __name__ == "__main__":
    analyze_quarterly_balance_sheet_coverage()
