import yfinance as yf
import pandas as pd
from datetime import datetime

def test_calendar_data(ticker_symbols):
    """Test calendar data retrieval for given ticker symbols."""
    results = {}
    
    for symbol in ticker_symbols:
        try:
            print(f"\n{'='*50}")
            print(f"Testing: {symbol}")
            print(f"{'='*50}")
            
            # Create ticker object
            ticker = yf.Ticker(f"{symbol}.NS")
            
            # Get calendar data (returns a dictionary)
            calendar = ticker.calendar
            
            if calendar:  # Check if dictionary is not empty
                print("\nCalendar Data:")
                print(calendar)
                
                # Get earnings dates
                try:
                    earnings_dates = ticker.get_earnings_dates()
                    if earnings_dates is not None and not earnings_dates.empty:
                        print("\nEarnings Dates:")
                        print(earnings_dates.head())
                    else:
                        print("\nNo earnings dates found")
                except Exception as e:
                    print(f"\nError getting earnings dates: {str(e)}")
                
                results[symbol] = {
                    'status': 'success',
                    'calendar_data': calendar,
                    'has_earnings_dates': earnings_dates is not None and not earnings_dates.empty
                }
            else:
                print("No calendar data available")
                results[symbol] = {'status': 'no_data'}
                
        except Exception as e:
            print(f"Error processing {symbol}: {str(e)}")
            results[symbol] = {'status': 'error', 'error': str(e)}
            
        # Add a small delay to avoid rate limiting
        import time
        time.sleep(2)
    
    return results

if __name__ == "__main__":
    # Test with a few large-cap stocks that likely have calendar data
    test_tickers = ["RELIANCE", "TCS", "HDFCBANK", "INFY", "HINDUNILVR"]
    
    print("Starting calendar data test...")
    results = test_calendar_data(test_tickers)
    
    print("\nTest Summary:")
    print("-" * 50)
    for ticker, result in results.items():
        print(f"{ticker}: {result['status']}")
