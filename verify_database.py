"""
Verify that the database schema was created correctly.
"""
import sys
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
import os

# Set console output encoding to UTF-8
if sys.stdout.encoding != 'utf-8':
    import io
    import sys
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def check_table_exists(conn, table_name):
    """Check if a table exists in the database"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """, (table_name,))
        return cur.fetchone()[0]

def get_table_columns(conn, table_name):
    """Get column information for a table"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        return cur.fetchall()

def main():
    # Load environment variables
    load_dotenv()
    
    # List of expected tables
    expected_tables = [
        'ticker',
        'balance_sheet_annual',
        'balance_sheet_quarterly',
        'income_statement_annual',
        'income_statement_quarterly',
        'cash_flow_annual',
        'cash_flow_quarterly',
        'price_history',
        'corporate_action'
    ]
    
    try:
        # Get database connection
        conn_str = os.getenv('DATABASE_URL')
        if conn_str and conn_str.startswith('DATABASE_URL='):
            conn_str = conn_str.split('=', 1)[1].strip()
        
        with psycopg.connect(conn_str) as conn:
            print("\n=== Database Connection Successful ===\n")
            
            # Check each expected table
            all_tables_exist = True
            for table in expected_tables:
                exists = check_table_exists(conn, table)
                status = "[OK]" if exists else "[MISSING]"
                print(f"{status} Table: {table}")
                
                if exists:
                    # Print column information
                    columns = get_table_columns(conn, table)
                    print(f"   Columns: {', '.join([col[0] for col in columns])}")
                else:
                    all_tables_exist = False
            
            if all_tables_exist:
                print("\n[SUCCESS] All expected tables exist!")
            else:
                print("\n[ERROR] Some tables are missing!")
                
    except Exception as e:
        print(f"\n[ERROR] Error verifying database: {e}")
    
    print("\n=== Verification Complete ===")

if __name__ == "__main__":
    main()
