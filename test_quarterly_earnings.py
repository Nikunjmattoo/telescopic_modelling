import yfinance as yf
import pandas as pd

# Test with a known ticker
test_ticker = "RELIANCE.NS"
ticker = yf.Ticker(test_ticker)

print("\n=== Testing ticker attributes ===")
print(f"Ticker: {test_ticker}")

# Check for quarterly_earnings and quarterly_income_stmt
print("\n=== Available attributes ===")
print(f"quarterly_earnings: {hasattr(ticker, 'quarterly_earnings')}")
print(f"quarterly_earnings type: {type(getattr(ticker, 'quarterly_earnings', None))}" if hasattr(ticker, 'quarterly_earnings') else "")

print(f"\nquarterly_income_stmt: {hasattr(ticker, 'quarterly_income_stmt')}")
print(f"quarterly_income_stmt type: {type(getattr(ticker, 'quarterly_income_stmt', None))}" if hasattr(ticker, 'quarterly_income_stmt') else "")

# Show what's actually available
print("\n=== All available attributes ===")
for attr in dir(ticker):
    if not attr.startswith('_') and not callable(getattr(ticker, attr)) and not attr.startswith('get_') and attr != 'ticker':
        print(f"- {attr}: {type(getattr(ticker, attr))}")
