#!/usr/bin/env python3
"""
Check the schema of the ticker table
"""
from db_utils import DatabaseConnection

def check_ticker_schema():
    """Check and display the schema of the ticker table"""
    try:
        db = DatabaseConnection()
        conn = db.connect()
        cursor = conn.cursor()
        
        # Get table structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'ticker'
            ORDER BY ordinal_position;
        """)
        
        print("\nTICKER TABLE SCHEMA:")
        print("-" * 60)
        print(f"{'Column':<20} {'Type':<20} {'Nullable':<10}")
        print("-" * 60)
        
        for col in cursor.fetchall():
            print(f"{col[0]:<20} {col[1]:<20} {col[2]:<10}")
        
        # Get row count
        cursor.execute("SELECT COUNT(*) FROM ticker")
        count = cursor.fetchone()[0]
        print("\nTotal tickers in database:", count)
        
        # Show first few tickers if any exist
        if count > 0:
            cursor.execute("SELECT * FROM ticker LIMIT 5")
            print("\nSample tickers:")
            for row in cursor.fetchall():
                print(f"- {row[0]}: {row[1]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error checking schema: {e}")

if __name__ == "__main__":
    check_ticker_schema()
