#!/usr/bin/env python3
"""
Test script for database connection - Telescopic Modelling Project
"""

from db_utils import DatabaseConnection
from dotenv import load_dotenv
import os

def test_connection():
    """Test database connection with better error handling"""
    print("Testing database connection...")
    
    try:
        # Load environment variables
        load_dotenv()
        
        # Create database connection
        db = DatabaseConnection()
        conn = db.connect()
        
        if conn:
            print("[SUCCESS] Connected to database!")
            
            # Get database info
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            print(f"PostgreSQL version: {version}")
            
            # List tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            tables = [row[0] for row in cursor.fetchall()]
            print(f"\nFound {len(tables)} tables:")
            for table in tables:
                print(f"- {table}")
                
            cursor.close()
            conn.close()
            return True
            
    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")
        return False
    
    return False

if __name__ == "__main__":
    print("=== DATABASE CONNECTION TEST ===")
    if test_connection():
        print("\n[SUCCESS] Database connection is working!")
    else:
        print("\n[ERROR] Could not connect to database.")
        print("Please check:")
        print("1. Is PostgreSQL running?")
        print("2. Are the credentials in .env file correct?")
        print("3. Can you connect using pgAdmin or psql?")
        print("\nCurrent connection string:")
        print(os.getenv('DATABASE_URL', 'Not found in .env'))
