#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script to verify ticker insertion into the database
"""
import sys
import io
import sys

# Set console output encoding to UTF-8
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from db_utils import DatabaseConnection

def test_ticker_insertion():
    """Test inserting sample tickers into the database"""
    # Sample tickers to test
    test_tickers = [
        "RELIANCE.NS",
        "TCS.NS",
        "HDFCBANK.NS",
        "INFY.NS",
        "HINDUNILVR.NS"
    ]
    
    try:
        # Initialize database connection
        db = DatabaseConnection()
        conn = db.connect()
        cursor = conn.cursor()
        
        # Clear existing data for clean test
        # First disable foreign key checks
        cursor.execute("SET session_replication_role = 'replica';")
        cursor.execute("TRUNCATE TABLE ticker CASCADE")
        cursor.execute("SET session_replication_role = 'origin';")
        conn.commit()
        print("[OK] Cleared existing ticker data")
        
        # Insert test tickers
        for ticker in test_tickers:
            cursor.execute(
                "INSERT INTO ticker (ticker) VALUES (%s) ON CONFLICT (ticker) DO NOTHING",
                (ticker,)
            )
            print(f"[OK] Inserted: {ticker}")
        
        # Commit changes
        conn.commit()
        print("\n[OK] Successfully committed changes")
        
        # Verify insertion
        cursor.execute("SELECT ticker FROM ticker ORDER BY ticker")
        saved_tickers = [row[0] for row in cursor.fetchall()]
        
        print("\nCurrent tickers in database:")
        for t in saved_tickers:
            print(f"- {t}")
            
        # Verify count
        if len(saved_tickers) == len(test_tickers):
            print("\n[OK] Test passed: All test tickers were inserted successfully!")
        else:
            print(f"\n⚠️  Warning: Expected {len(test_tickers)} tickers, found {len(saved_tickers)}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n[ERROR] Error during test: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False
    
    return True

if __name__ == "__main__":
    print("=== TICKER INSERTION TEST ===\n")
    success = test_ticker_insertion()
    sys.exit(0 if success else 1)
