import os
import json
import pandas as pd
from datetime import datetime, timedelta
import glob

def analyze_financials_coverage():
    """Analyze coverage of financials data across companies."""
    print("Analyzing financials coverage...")
    
    # Get list of all financials files
    files = glob.glob('data/financials/*.json')
    if not files:
        print("No financials data found. Please run download_financials.py first.")
        return
    
    # Initialize results
    results = []
    
    # Analyze each file
    for file in tqdm(files, desc="Processing files"):
        try:
            with open(file, 'r') as f:
                data = json.load(f)
            
            ticker = os.path.basename(file).replace('.json', '')
            has_income = bool(data.get('income_statement'))
            has_balance_sheet = bool(data.get('balance_sheet', {}))
            has_cash_flow = bool(data.get('cash_flow', {}))
            
            # Get years with data
            years = set()
            for stmt in ['income_statement', 'balance_sheet', 'cash_flow']:
                if stmt in data and data[stmt]:
                    years.update(data[stmt].keys())
            
            # Get most recent year with data
            latest_year = max(years) if years else None
            
            results.append({
                'ticker': ticker,
                'has_income': has_income,
                'has_balance_sheet': has_balance_sheet,
                'has_cash_flow': has_cash_flow,
                'years_available': len(years),
                'latest_year': latest_year,
                'last_updated': data.get('last_updated', '')
            })
            
        except Exception as e:
            print(f"Error processing {file}: {str(e)}")
    
    if not results:
        print("No valid financials data found.")
        return
    
    # Create DataFrame
    df = pd.DataFrame(results)
    
    # Save detailed report
    os.makedirs('reports', exist_ok=True)
    detailed_path = os.path.join('reports', 'financials_detailed_report.csv')
    df.to_csv(detailed_path, index=False)
    
    # Generate summary statistics
    summary = {
        'total_companies': len(df),
        'with_income_statement': df['has_income'].sum(),
        'with_balance_sheet': df['has_balance_sheet'].sum(),
        'with_cash_flow': df['has_cash_flow'].sum(),
        'avg_years_available': df['years_available'].mean(),
        'min_years': df['years_available'].min(),
        'max_years': df['years_available'].max(),
        'latest_year_available': df['latest_year'].max(),
        'report_generated': datetime.now().isoformat()
    }
    
    # Save summary
    summary_path = os.path.join('reports', 'financials_summary.json')
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Print summary
    print("\n=== FINANCIALS COVERAGE SUMMARY ===")
    print(f"Total companies analyzed: {summary['total_companies']}")
    print(f"Companies with income statement: {summary['with_income_statement']} ({summary['with_income_statement']/summary['total_companies']*100:.1f}%)")
    print(f"Companies with balance sheet: {summary['with_balance_sheet']} ({summary['with_balance_sheet']/summary['total_companies']*100:.1f}%)")
    print(f"Companies with cash flow: {summary['with_cash_flow']} ({summary['with_cash_flow']/summary['total_companies']*100:.1f}%)")
    print(f"Average years of data: {summary['avg_years_available']:.1f} (min: {summary['min_years']}, max: {summary['max_years']})")
    print(f"Latest year with data: {summary['latest_year_available']}")
    print(f"\nDetailed report: {os.path.abspath(detailed_path)}")
    print(f"Summary report: {os.path.abspath(summary_path)}")
    
    return df, summary

if __name__ == "__main__":
    from tqdm import tqdm
    analyze_financials_coverage()
