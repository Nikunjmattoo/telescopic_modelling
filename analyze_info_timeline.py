import os
import json
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import glob

def analyze_info_timeline():
    """Analyze company information availability over time in quarterly format."""
    # Directory containing company info files
    info_dir = os.path.join('data', 'info')
    
    # Check if directory exists
    if not os.path.exists(info_dir):
        print(f"Error: Directory {info_dir} not found.")
        return None
    
    # Get all JSON files
    info_files = glob.glob(os.path.join(info_dir, '*.json'))
    
    if not info_files:
        print("No company info files found.")
        return None
    
    print(f"Analyzing {len(info_files)} company info files...")
    
    # Initialize quarterly coverage
    current_date = datetime.now()
    start_date = datetime(2015, 1, 1)  # Start from 2015 as per price history
    
    # Generate quarters from start_date to current_date
    quarters = []
    current_quarter = start_date
    while current_quarter <= current_date:
        quarters.append(current_quarter)
        # Move to next quarter
        if current_quarter.month in [1, 2, 3]:
            current_quarter = current_quarter.replace(month=4, day=1)
        elif current_quarter.month in [4, 5, 6]:
            current_quarter = current_quarter.replace(month=7, day=1)
        elif current_quarter.month in [7, 8, 9]:
            current_quarter = current_quarter.replace(month=10, day=1)
        else:
            current_quarter = current_quarter.replace(year=current_quarter.year + 1, month=1, day=1)
    
    # Initialize coverage DataFrame
    coverage = pd.DataFrame(index=[f"{q.year} Q{(q.month-1)//3 + 1}" for q in quarters],
                          columns=['Total_Companies', 'Companies_Available', 'Avg_Completeness'])
    coverage.index.name = 'Quarter'
    coverage = coverage.fillna(0)
    
    # Define important fields to track
    important_fields = [
        'sector', 'industry', 'marketCap', 'country', 'exchange',
        'fullTimeEmployees', 'longBusinessSummary', 'auditRisk', 
        'boardRisk', 'compensationRisk', 'shareHolderRightsRisk',
        'overallRisk', 'lastFiscalYearEnd', 'mostRecentQuarter',
        'profitMargins', 'revenueGrowth', 'earningsQuarterlyGrowth',
        'totalRevenue', 'ebitda', 'grossProfits', 'freeCashflow',
        'operatingCashflow', 'debtToEquity', 'returnOnEquity',
        'returnOnAssets', 'dividendYield', 'payoutRatio'
    ]
    
    # Track company counts and completeness by quarter
    for q_start in quarters:
        q_end = q_start + timedelta(days=90)  # Approximate quarter length
        q_str = f"{q_start.year} Q{(q_start.month-1)//3 + 1}"
        
        # Skip future quarters
        if q_start > current_date:
            continue
            
        companies_in_quarter = 0
        total_completeness = 0
        
        for file_path in tqdm(info_files, desc=f"Processing {q_str}"):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Get last updated date - handle different date formats
                last_updated_str = data.get('last_updated', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                try:
                    # Try with time first
                    last_updated = datetime.strptime(last_updated_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        # Try date only
                        last_updated = datetime.strptime(last_updated_str, '%Y-%m-%d')
                    except ValueError:
                        # If still can't parse, skip this file
                        continue
                
                # Check if this data was available in this quarter
                if last_updated <= q_end:
                    companies_in_quarter += 1
                    
                    # Calculate completeness for this company
                    fields_present = sum(1 for field in important_fields if field in data and data[field] is not None)
                    completeness = (fields_present / len(important_fields)) * 100 if important_fields else 0
                    total_completeness += completeness
                
            except Exception as e:
                # Skip files with errors
                continue
        
        # Update coverage DataFrame
        if companies_in_quarter > 0:
            coverage.at[q_str, 'Total_Companies'] = companies_in_quarter
            coverage.at[q_str, 'Companies_Available'] = companies_in_quarter
            coverage.at[q_str, 'Avg_Completeness'] = total_completeness / companies_in_quarter if companies_in_quarter > 0 else 0
    
    # Calculate percentages
    coverage['Available_Percentage'] = (coverage['Companies_Available'] / coverage['Total_Companies']) * 100
    coverage['Completeness_Percentage'] = coverage['Avg_Completeness']
    
    # Filter out quarters with no data
    coverage = coverage[coverage['Companies_Available'] > 0]
    
    # Save to CSV in the same format as price history report
    output_path = os.path.join('data', 'info_coverage_report.csv')
    coverage.to_csv(output_path)
    
    # Print summary
    print("\n=== COMPANY INFORMATION COVERAGE REPORT ===")
    print(f"Report saved to: {os.path.abspath(output_path)}")
    print("\nLatest Quarter Summary:")
    print(coverage[['Companies_Available', 'Available_Percentage', 'Avg_Completeness']].tail().to_string())
    
    return coverage

if __name__ == "__main__":
    analyze_info_timeline()
