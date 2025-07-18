import pandas as pd
import os
from datetime import datetime
import glob
from tqdm import tqdm

def analyze_quarterly_coverage():
    # Directory containing the price history files
    data_dir = 'data/price_history'
    
    # Get list of all CSV files
    csv_files = glob.glob(os.path.join(data_dir, '*.csv'))
    
    # Define the quarters we're interested in (2015 Q1 to current quarter)
    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1
    quarters = []
    
    # Generate all quarters from 2015 Q1 to current quarter
    for year in range(2015, current_year + 1):
        end_q = 4 if year < current_year else current_quarter
        for q in range(1, end_q + 1):
            quarters.append((year, q))
    
    # Create a DataFrame to store the results
    coverage = pd.DataFrame(index=[f"{y} Q{q}" for y, q in quarters], 
                          columns=['Total_Trading_Days', 'Stocks_Available', 'Stocks_Complete',
                                 'Avg_Completeness', 'Min_Completeness', 'Max_Completeness'])
    
    # Calculate expected trading days per quarter (approximate)
    trading_days_per_quarter = 63  # Rough average
    
    # Initialize counters
    for quarter in coverage.index:
        coverage.at[quarter, 'Total_Trading_Days'] = trading_days_per_quarter
        coverage.at[quarter, 'Stocks_Available'] = 0
        coverage.at[quarter, 'Stocks_Complete'] = 0
        coverage.at[quarter, 'Avg_Completeness'] = 0.0
        coverage.at[quarter, 'Min_Completeness'] = 100.0
        coverage.at[quarter, 'Max_Completeness'] = 0.0
    
    def analyze_stock_data(csv_files, quarters):
        """Analyze stock data and generate quarterly coverage report."""
        coverage_data = []
        
        print("Analyzing stock data...")
        for file_path in tqdm(csv_files):
            try:
                # Read the CSV file, skipping the first 3 rows of metadata
                df = pd.read_csv(file_path, skiprows=3, header=None, 
                               names=['Date', 'Close', 'High', 'Low', 'Open', 'Volume', 'Ticker'])
                
                # Convert date column to datetime
                df['Date'] = pd.to_datetime(df['Date'])
                
                # Process each quarter
                for year, q in quarters:
                    # Define quarter start and end dates
                    q_start = pd.Timestamp(year=year, month=3*q-2, day=1)
                    if q < 4:
                        q_end = pd.Timestamp(year=year, month=3*q+1, day=1) - pd.Timedelta(days=1)
                    else:
                        q_end = pd.Timestamp(year=year+1, month=1, day=1) - pd.Timedelta(days=1)
                    
                    # Filter data for the quarter
                    q_data = df[(df['Date'] >= q_start) & (df['Date'] <= q_end)]
                    
                    # Check if we have any data for this quarter
                    if len(q_data) > 0:
                        quarter_str = f"{year} Q{q}"
                        
                        # Calculate completeness percentage for this stock in this quarter
                        completeness_pct = (len(q_data) / trading_days_per_quarter) * 100
                        
                        # Update coverage metrics
                        coverage.at[quarter_str, 'Stocks_Available'] += 1
                        coverage.at[quarter_str, 'Avg_Completeness'] += completeness_pct
                        
                        # Update min/max completeness
                        if completeness_pct < coverage.at[quarter_str, 'Min_Completeness']:
                            coverage.at[quarter_str, 'Min_Completeness'] = completeness_pct
                        if completeness_pct > coverage.at[quarter_str, 'Max_Completeness']:
                            coverage.at[quarter_str, 'Max_Completeness'] = completeness_pct
                        
                        # Check if we have enough data points (at least 80% of expected trading days)
                        min_required = int(trading_days_per_quarter * 0.8)  # At least 80% of expected days
                        if len(q_data) >= min_required:
                            coverage.at[quarter_str, 'Stocks_Complete'] += 1
                
                # Add to coverage data for detailed reporting
                ticker = os.path.basename(file_path).replace('.csv', '')
                coverage_data.append({
                    'Ticker': ticker,
                    **{f"{year}_Q{q}": len(df[(df['Date'] >= pd.Timestamp(year=year, month=3*q-2, day=1)) & 
                                             (df['Date'] <= (pd.Timestamp(year=year, month=3*q+1, day=1) - pd.Timedelta(days=1)) 
                                              if q < 4 else pd.Timestamp(year=year+1, month=1, day=1) - pd.Timedelta(days=1))]) 
                       for year, q in quarters}
                })
                
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
                continue
    
    analyze_stock_data(csv_files, quarters)
    
    # Calculate final metrics
    coverage['Available_Percentage'] = (coverage['Stocks_Available'] / len(csv_files)) * 100
    coverage['Complete_Percentage'] = (coverage['Stocks_Complete'] / len(csv_files)) * 100
    
    # Calculate average completeness (as percentage of stocks with data)
    for quarter in coverage.index:
        if coverage.at[quarter, 'Stocks_Available'] > 0:
            coverage.at[quarter, 'Avg_Completeness'] = (
                coverage.at[quarter, 'Avg_Completeness'] / coverage.at[quarter, 'Stocks_Available']
            )
        else:
            coverage.at[quarter, 'Avg_Completeness'] = 0.0
            coverage.at[quarter, 'Min_Completeness'] = 0.0
            coverage.at[quarter, 'Max_Completeness'] = 0.0
    
    # Save the report
    report_path = 'data/quarterly_coverage_report.csv'
    os.makedirs('data', exist_ok=True)
    coverage.to_csv(report_path)
    print(f"Quarterly coverage report saved to {report_path}")
    
    return coverage

if __name__ == "__main__":
    report = analyze_quarterly_coverage()
    print("\nQuarterly Coverage & Completeness Summary:")
    print("Coverage = % of stocks with any data")
    print("Completeness = Average % of trading days available per stock\n")
    
    # Format for better readability
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.options.display.float_format = '{:.1f}%'.format
    
    # Select and rename columns for display
    display_cols = {
        'Stocks_Available': 'Stocks',
        'Available_Percentage': 'Coverage',
        'Complete_Percentage': 'Complete',
        'Avg_Completeness': 'Avg Days %',
        'Min_Completeness': 'Min Days %',
        'Max_Completeness': 'Max Days %'
    }
    
    # Convert percentage columns and format
    display_df = report[list(display_cols.keys())].copy()
    display_df.rename(columns=display_cols, inplace=True)
    
    print(display_df)
    
    # Print summary statistics
    print("\nSummary Statistics:")
    stats = display_df[['Coverage', 'Complete', 'Avg Days %']].describe()
    print(stats)
