import os
import psycopg
from psycopg import sql
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

def get_connection():
    """Get a database connection using psycopg3"""
    conn_str = os.getenv('DATABASE_URL')
    if not conn_str:
        raise ValueError("DATABASE_URL environment variable not set")
    
    # Handle the case where connection string is in DATABASE_URL= format
    if conn_str.startswith('DATABASE_URL='):
        conn_str = conn_str.split('=', 1)[1].strip()
    
    return psycopg.connect(conn_str)

def setup_database():
    """Set up the database schema by executing the schema.sql file"""
    # Read the schema file
    schema_file = Path(__file__).parent / 'schema.sql'
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")
    
    with open(schema_file, 'r') as f:
        schema_sql = f.read()
    
    # Connect to the database and execute the schema
    with get_connection() as conn:
        with conn.cursor() as cur:
            print("Executing schema...")
            cur.execute(schema_sql)
            conn.commit()
            print("Database schema created successfully!")

def main():
    try:
        setup_database()
        print("\nDatabase setup completed successfully!")
    except Exception as e:
        print(f"\nError setting up database: {e}")

if __name__ == "__main__":
    main()
