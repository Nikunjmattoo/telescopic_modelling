import yfinance as yf
import json
from datetime import datetime
import pandas as pd

def convert_timestamps(obj):
    """Recursively convert Timestamp objects to strings."""
    if isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d')
    elif isinstance(obj, dict):
        return {str(k): convert_timestamps(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_timestamps(x) for x in obj]
    return obj

def test_ticker(ticker_symbol):
    """Test downloading financials for a single ticker."""
    try:
        print(f"\nTesting {ticker_symbol}...")
        ticker = yf.Ticker(f"{ticker_symbol}.NS")
        
        # Test basic info
        info = ticker.info
        print(f"Company: {info.get('longName', 'N/A')}")
        print(f"Sector: {info.get('sector', 'N/A')}")
        
        # Test financial statements
        financials = ticker.financials
        balance_sheet = ticker.balance_sheet
        cash_flow = ticker.cashflow
        
        print(f"Financials shape: {financials.shape if financials is not None else 'None'}")
        print(f"Balance Sheet shape: {balance_sheet.shape if balance_sheet is not None else 'None'}")
        print(f"Cash Flow shape: {cash_flow.shape if cash_flow is not None else 'None'}")
        
        if financials is not None and not financials.empty:
            # Save a sample of the data with timestamps converted to strings
            sample_data = {
                'ticker': ticker_symbol,
                'company_name': info.get('longName', ticker_symbol),
                'financials_columns': convert_timestamps(list(financials.columns)),
                'financials_index': convert_timestamps(list(financials.index)),
                'sample_data': convert_timestamps(financials.iloc[:5, :5].to_dict())
            }
            with open(f'data/financials/{ticker_symbol}_sample.json', 'w') as f:
                json.dump(sample_data, f, indent=2)
            print(f"Saved sample data for {ticker_symbol}")
            return True
        return False
        
    except Exception as e:
        print(f"Error testing {ticker_symbol}: {str(e)}")
        return False

if __name__ == "__main__":
    # Test with some known good Indian companies
    test_tickers = [
        'RELIANCE',  # Reliance Industries
        'TCS',       # Tata Consultancy Services
        'HDFCBANK',  # HDFC Bank
        'INFY',      # Infosys
        'HINDUNILVR' # Hindustan Unilever
    ]
    
    print("Starting financials download test...")
    results = {}
    for ticker in test_tickers:
        results[ticker] = test_ticker(ticker)
    
    print("\nTest Results:")
    for ticker, success in results.items():
        print(f"{ticker}: {'SUCCESS' if success else 'FAILED'}")
