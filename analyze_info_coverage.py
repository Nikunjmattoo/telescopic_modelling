import os
import json
import pandas as pd
from datetime import datetime

def analyze_info_coverage():
    """Analyze the completeness of downloaded company information."""
    # Directory containing company info files
    info_dir = os.path.join('data', 'info')
    
    # Check if directory exists
    if not os.path.exists(info_dir):
        print(f"Error: Directory {info_dir} not found.")
        return None
    
    # Get all JSON files
    info_files = [f for f in os.listdir(info_dir) if f.endswith('.json')]
    
    if not info_files:
        print("No company info files found.")
        return None
    
    print(f"Analyzing {len(info_files)} company info files...")
    
    # Define important fields to check (aligned with yfinance's info keys)
    important_fields = [
        'sector', 'industry', 'marketCap', 'country', 'exchange',
        'fullTimeEmployees', 'longBusinessSummary', 'auditRisk', 
        'boardRisk', 'compensationRisk', 'shareHolderRightsRisk',
        'overallRisk', 'lastFiscalYearEnd', 'mostRecentQuarter',
        'profitMargins', 'revenueGrowth', 'earningsQuarterlyGrowth',
        'totalRevenue', 'ebitda', 'grossProfits', 'freeCashflow',
        'operatingCashflow', 'debtToEquity', 'returnOnEquity',
        'returnOnAssets', 'dividendYield', 'payoutRatio',
        'currentPrice', 'targetHighPrice', 'targetLowPrice',
        'targetMeanPrice', 'targetMedianPrice', 'recommendationKey'
    ]
    
    # Initialize results
    results = []
    field_stats = {field: {'present': 0, 'non_null': 0} for field in important_fields}
    
    # Process each file
    for file in info_files:
        file_path = os.path.join(info_dir, file)
        ticker = os.path.splitext(file)[0]
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check each important field
            present_fields = {}
            for field in important_fields:
                if field in data:
                    field_stats[field]['present'] += 1
                    if data[field] is not None:
                        field_stats[field]['non_null'] += 1
                    present_fields[field] = data[field] is not None
                else:
                    present_fields[field] = False
            
            # Add to results
            results.append({
                'ticker': ticker,
                'fields_present': sum(1 for v in present_fields.values() if v),
                'total_fields': len(important_fields),
                **present_fields
            })
            
        except Exception as e:
            print(f"Error processing {file}: {str(e)}")
    
    # Create DataFrames
    df = pd.DataFrame(results)
    
    # Calculate field statistics
    field_summary = []
    for field, stats in field_stats.items():
        presence = (stats['present'] / len(info_files)) * 100
        non_null = (stats['non_null'] / stats['present'] * 100) if stats['present'] > 0 else 0
        field_summary.append({
            'Field': field,
            'Presence (%)': round(presence, 1),
            'Non-Null (%)': round(non_null, 1) if stats['present'] > 0 else 0,
            'Present': stats['present'],
            'Non-Null': stats['non_null']
        })
    
    field_df = pd.DataFrame(field_summary).sort_values('Presence (%)', ascending=False)
    
    # Calculate overall statistics
    df['completeness'] = (df['fields_present'] / df['total_fields']) * 100
    
    # Print summary
    print("\n=== COMPANY INFORMATION COVERAGE REPORT ===")
    print(f"Total Companies: {len(df)}")
    print(f"Average Completeness: {df['completeness'].mean():.1f}%")
    print(f"Companies with >80% completeness: {len(df[df['completeness'] > 80])} ({(len(df[df['completeness'] > 80])/len(df)*100):.1f}%)")
    
    # Print field statistics
    print("\n=== FIELD COMPLETENESS (Top 20) ===")
    print(field_df[['Field', 'Presence (%)', 'Non-Null (%)']].head(20).to_string(index=False))
    
    # Save detailed report
    report_dir = 'reports'
    os.makedirs(report_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(report_dir, f'info_coverage_report_{timestamp}.xlsx')
    
    with pd.ExcelWriter(report_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Company Details', index=False)
        field_df.to_excel(writer, sheet_name='Field Statistics', index=False)
    
    print(f"\nDetailed report saved to: {os.path.abspath(report_path)}")
    
    return df, field_df

if __name__ == "__main__":
    analyze_info_coverage()
