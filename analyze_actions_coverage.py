import os
import json
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import glob

def analyze_actions_coverage():
    """Analyze actions data coverage in quarterly format."""
    # Directory containing actions files
    actions_dir = os.path.join('data', 'actions')
    
    # Check if directory exists
    if not os.path.exists(actions_dir):
        print(f"Error: Directory {actions_dir} not found. Please run download_actions.py first.")
        return None
    
    # Get all JSON files
    actions_files = glob.glob(os.path.join(actions_dir, '*.json'))
    
    if not actions_files:
        print("No actions files found. Please run download_actions.py first.")
        return None
    
    print(f"Analyzing {len(actions_files)} actions files...")
    
    # Initialize quarterly coverage
    current_date = datetime.now()
    start_date = datetime(2015, 1, 1)  # Start from 2015 as per previous analysis
    
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
                          columns=['Total_Companies', 'Companies_With_Actions', 'Companies_With_Dividends', 
                                 'Companies_With_Splits', 'Last_Updated'])
    coverage.index.name = 'Quarter'
    coverage = coverage.fillna(0)
    
    # Track company counts and actions by quarter
    for q_start in quarters:
        q_end = q_start + timedelta(days=90)  # Approximate quarter length
        q_str = f"{q_start.year} Q{(q_start.month-1)//3 + 1}"
        
        # Skip future quarters
        if q_start > current_date:
            continue
            
        companies_with_actions = 0
        companies_with_dividends = 0
        companies_with_splits = 0
        last_updated = None
        
        for file_path in tqdm(actions_files, desc=f"Processing {q_str}"):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Get last updated date
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
                    has_actions = False
                    has_dividends = False
                    has_splits = False
                    
                    if data.get('dividends'):
                        has_actions = True
                        has_dividends = True
                    if data.get('splits'):
                        has_actions = True
                        has_splits = True
                    
                    if has_actions:
                        companies_with_actions += 1
                    if has_dividends:
                        companies_with_dividends += 1
                    if has_splits:
                        companies_with_splits += 1
                
            except Exception as e:
                # Skip files with errors
                continue
        
        # Update coverage DataFrame
        if companies_with_actions > 0:
            coverage.at[q_str, 'Total_Companies'] = len(actions_files)
            coverage.at[q_str, 'Companies_With_Actions'] = companies_with_actions
            coverage.at[q_str, 'Companies_With_Dividends'] = companies_with_dividends
            coverage.at[q_str, 'Companies_With_Splits'] = companies_with_splits
            coverage.at[q_str, 'Last_Updated'] = q_end.strftime('%Y-%m-%d')
    
    # Calculate percentages
    coverage['Actions_Coverage_Percent'] = (coverage['Companies_With_Actions'] / coverage['Total_Companies']) * 100
    coverage['Dividends_Coverage_Percent'] = (coverage['Companies_With_Dividends'] / coverage['Total_Companies']) * 100
    coverage['Splits_Coverage_Percent'] = (coverage['Companies_With_Splits'] / coverage['Total_Companies']) * 100
    
    # Filter out quarters with no data
    coverage = coverage[coverage['Companies_With_Actions'] > 0]
    
    # Save to CSV
    output_path = os.path.join('data', 'actions_coverage_report.csv')
    coverage.to_csv(output_path)
    
    # Print summary
    print("\n=== ACTIONS DATA COVERAGE REPORT ===")
    print(f"Report saved to: {os.path.abspath(output_path)}")
    print("\nLatest Quarter Summary:")
    print(coverage[['Companies_With_Actions', 'Companies_With_Dividends', 
                   'Companies_With_Splits', 'Actions_Coverage_Percent']].tail().to_string())
    
    return coverage

if __name__ == "__main__":
    analyze_actions_coverage()
