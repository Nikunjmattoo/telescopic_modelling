import yfinance as yf
import pandas as pd

# Test with a known ticker
test_ticker = "RELIANCE.NS"
ticker = yf.Ticker(test_ticker)

print(f"\n=== Testing quarterly_income_stmt for {test_ticker} ===")

# Check if quarterly_income_stmt exists and show its contents
if hasattr(ticker, 'quarterly_income_stmt') and ticker.quarterly_income_stmt is not None:
    print("\n=== Quarterly Income Statement ===")
    print(ticker.quarterly_income_stmt.head())
    print("\n=== Data Types ===")
    print(ticker.quarterly_income_stmt.dtypes)
    print("\n=== Index (Dates) ===")
    print(ticker.quarterly_income_stmt.columns)
else:
    print("quarterly_income_stmt is not available or returns None")
