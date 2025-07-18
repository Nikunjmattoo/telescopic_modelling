import os
import json
import pandas as pd
from datetime import datetime, timedelta
import glob

def load_actions_coverage():
    """Load and process actions coverage data."""
    try:
        df = pd.read_csv(os.path.join('data', 'actions_coverage_report.csv'), index_col='Quarter')
        df = df[['Companies_With_Actions', 'Companies_With_Dividends', 'Companies_With_Splits']]
        df.columns = ['Actions_Count', 'Dividends_Count', 'Splits_Count']
        return df
    except FileNotFoundError:
        print("Actions coverage report not found. Run analyze_actions_coverage.py first.")
        return None

def load_info_coverage():
    """Load and process info coverage data."""
    try:
        df = pd.read_csv(os.path.join('data', 'info_coverage_report.csv'), index_col='Quarter')
        df = df[['Companies_Available', 'Avg_Completeness']]
        df.columns = ['Info_Count', 'Info_Completeness']
        return df
    except FileNotFoundError:
        print("Info coverage report not found. Run analyze_info_coverage.py first.")
        return None

def load_price_history_coverage():
    """Load and process price history coverage data."""
    try:
        df = pd.read_csv(os.path.join('data', 'quarterly_coverage_report.csv'))
        # The first column is the index (Quarter)
        df = df.rename(columns={'Unnamed: 0': 'Quarter'})
        df = df.set_index('Quarter')
        df = df[['Stocks_Available', 'Stocks_Complete', 'Avg_Completeness']]
        df.columns = ['Price_History_Available', 'Price_History_Complete', 'Price_History_Completeness']
        return df
    except Exception as e:
        print(f"Error loading price history coverage: {str(e)}")
        return None

def generate_unified_report():
    """Generate a unified coverage report across all data modules."""
    print("Generating unified coverage report...")
    
    # Load all coverage reports
    actions_df = load_actions_coverage()
    info_df = load_info_coverage()
    price_df = load_price_history_coverage()
    
    # Start with the price history as our base since it has the longest history
    combined_df = price_df if price_df is not None else pd.DataFrame()
    
    # Add other modules' data if available
    if actions_df is not None:
        if combined_df.empty:
            combined_df = actions_df
        else:
            combined_df = combined_df.join(actions_df, how='outer')
    
    if info_df is not None:
        combined_df = combined_df.join(info_df, how='outer')
    
    if combined_df.empty:
        print("No coverage data found. Please run individual coverage scripts first.")
        return
    
    # Calculate coverage percentages based on the maximum count for each module
    if 'Actions_Count' in combined_df.columns:
        max_actions = combined_df['Actions_Count'].max()
        combined_df['Actions_Coverage_Pct'] = (combined_df['Actions_Count'] / max_actions * 100).round(2)
    
    if 'Info_Count' in combined_df.columns:
        max_info = combined_df['Info_Count'].max()
        combined_df['Info_Coverage_Pct'] = (combined_df['Info_Count'] / max_info * 100).round(2)
    
    if 'Price_History_Available' in combined_df.columns:
        max_price = combined_df['Price_History_Available'].max()
        combined_df['Price_History_Coverage_Pct'] = (combined_df['Price_History_Available'] / max_price * 100).round(2)
    
    # Reorder columns for better readability
    module_order = ['Actions', 'Info', 'Price_History']
    ordered_cols = []
    
    for module in module_order:
        # Add count columns
        for suffix in ['_Count', '_Available', '_Complete']:
            col = f"{module}{suffix}"
            if col in combined_df.columns and col not in ordered_cols:
                ordered_cols.append(col)
        
        # Add coverage/percentage columns
        for suffix in ['_Coverage_Pct', '_Completeness']:
            col = f"{module}{suffix}"
            if col in combined_df.columns and col not in ordered_cols:
                ordered_cols.append(col)
    
    # Add any remaining columns
    for col in combined_df.columns:
        if col not in ordered_cols:
            ordered_cols.append(col)
    
    combined_df = combined_df[ordered_cols].sort_index()
    
    # Save the report
    os.makedirs('reports', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join('reports', f'unified_coverage_report_{timestamp}.xlsx')
    
    with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
        # Full data sheet
        combined_df.to_excel(writer, sheet_name='Quarterly Coverage')
        
        # Create a summary sheet with key metrics
        summary_data = []
        
        # Actions summary
        if 'Actions_Count' in combined_df.columns:
            latest_actions = combined_df['Actions_Count'].iloc[-1]
            max_actions = combined_df['Actions_Count'].max()
            coverage_pct = (latest_actions / max_actions * 100).round(2)
            summary_data.append({
                'Module': 'Actions',
                'Latest Count': latest_actions,
                'Max Count': max_actions,
                'Coverage %': coverage_pct,
                'Latest Quarter': combined_df.index[-1]
            })
        
        # Info summary
        if 'Info_Count' in combined_df.columns:
            latest_info = combined_df['Info_Count'].iloc[-1]
            max_info = combined_df['Info_Count'].max()
            coverage_pct = (latest_info / max_info * 100).round(2)
            completeness = combined_df.get('Info_Completeness', [0]).iloc[-1]
            
            summary_data.append({
                'Module': 'Company Info',
                'Latest Count': latest_info,
                'Max Count': max_info,
                'Coverage %': coverage_pct,
                'Completeness %': completeness,
                'Latest Quarter': combined_df.index[-1]
            })
        
        # Price History summary
        if 'Price_History_Available' in combined_df.columns:
            latest_available = combined_df['Price_History_Available'].iloc[-1]
            latest_complete = combined_df.get('Price_History_Complete', [latest_available]).iloc[-1]
            completeness = combined_df.get('Price_History_Completeness', [0]).iloc[-1]
            
            summary_data.append({
                'Module': 'Price History',
                'Available': latest_available,
                'Complete': latest_complete,
                'Completeness %': completeness,
                'Latest Quarter': combined_df.index[-1]
            })
        
        # Create and save summary dataframe
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Auto-adjust column widths in summary sheet
            worksheet = writer.sheets['Summary']
            for idx, col in enumerate(summary_df.columns):
                max_length = max(
                    summary_df[col].astype(str).apply(len).max(),
                    len(str(col))
                ) + 2  # Add a little extra space
                worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 30)
    
    print(f"\n=== UNIFIED COVERAGE REPORT ===")
    print(f"Report saved to: {os.path.abspath(report_path)}")
    print("\nLatest Quarter Summary:")
    print(combined_df.tail(1).to_string())
    
    return combined_df

if __name__ == "__main__":
    generate_unified_report()
