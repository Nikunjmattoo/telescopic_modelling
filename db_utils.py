"""
Database utility for PostgreSQL connection - Telescopic Modelling Project
"""
import psycopg
from psycopg.rows import dict_row
import os
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class DatabaseConnection:
    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize database connection
        
        Args:
            connection_string: PostgreSQL connection string
                              If None, will try to get from environment variable DATABASE_URL
        """
        self.connection_string = connection_string or os.getenv('DATABASE_URL')
        if not self.connection_string:
            raise ValueError("Database connection string not provided. Set DATABASE_URL environment variable or pass connection_string parameter.")
        
        self.connection = None
        self.db_info = self._parse_connection_string()
    
    def _parse_connection_string(self):
        """Parse connection string into components"""
        try:
            # Handle the case where connection string is in DATABASE_URL format
            if 'DATABASE_URL=' in self.connection_string:
                conn_str = self.connection_string.split('DATABASE_URL=')[1].split(' ')[0].strip()
                result = urlparse(conn_str)
            else:
                result = urlparse(self.connection_string)
                
            # Extract username and password from netloc if they exist
            username = None
            password = None
            if result.username:
                username = result.username
            if result.password:
                password = result.password
                
            # Handle the case where username:password is in the netloc
            if '@' in result.netloc and ':' in result.netloc.split('@')[0]:
                auth = result.netloc.split('@')[0]
                if ':' in auth:
                    username, password = auth.split(':', 1)
            
            return {
                'host': result.hostname or 'localhost',
                'port': result.port or 5432,
                'database': result.path[1:],  # Remove leading '/'
                'username': username,
                'password': password
            }
        except Exception as e:
            raise ValueError(f"Invalid connection string: {e}")
    
    def connect(self):
        """Establish database connection"""
        try:
            # For psycopg3, we can use the connection string directly
            conn_str = self.connection_string
            if 'DATABASE_URL=' in conn_str:
                conn_str = conn_str.split('DATABASE_URL=')[1].strip()
                
            self.connection = psycopg.connect(conn_str)
            self.connection.autocommit = False
            return self.connection
        except Exception as e:
            print(f"Database connection failed: {e}")
            raise
    
    def get_cursor(self, dict_cursor=True):
        """Get database cursor"""
        if not self.connection:
            self.connect()
        
        if dict_cursor:
            return self.connection.cursor(row_factory=dict_row)
        else:
            return self.connection.cursor()
    
    def execute_query(self, query: str, params=None, fetch=False) -> Any:
        """Execute a query"""
        cursor = self.get_cursor()
        try:
            cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
            else:
                self.connection.commit()
                return cursor.rowcount
        except Exception as e:
            self.connection.rollback()
            print(f"Query execution failed: {e}")
            raise
        finally:
            cursor.close()
    
    def test_connection(self) -> Dict[str, Any]:
        """Test database connection and return connection info"""
        try:
            cursor = self.get_cursor()
            cursor.execute("SELECT version(), current_database(), current_user, now();")
            result = cursor.fetchone()
            cursor.close()
            
            return {
                'status': 'success',
                'database': result['current_database'],
                'user': result['current_user'],
                'timestamp': result['now'],
                'version': result['version'][:50] + '...' if len(result['version']) > 50 else result['version'],
                'connection_info': self.db_info
            }
        except Exception as e:
            return {
                'status': 'failed',
                'error': str(e),
                'connection_info': self.db_info
            }
    
    def get_tables(self) -> List[str]:
        """Get list of tables in database"""
        try:
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
            """
            result = self.execute_query(query, fetch=True)
            return [row['table_name'] for row in result]
        except Exception as e:
            print(f"Failed to get tables: {e}")
            return []
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None

def get_db_connection() -> DatabaseConnection:
    """Get a database connection instance"""
    return DatabaseConnection()

def test_database_connection():
    """Test database connection and print results"""
    try:
        db = get_db_connection()
        result = db.test_connection()
        
        print("=== DATABASE CONNECTION TEST ===")
        if result['status'] == 'success':
            print("[SUCCESS] Connection successful!")
            print(f"Database: {result['database']}")
            print(f"User: {result['user']}")
            print(f"Timestamp: {result['timestamp']}")
            print(f"Version: {result['version']}")
            print(f"Host: {result['connection_info'].get('host', 'Unknown')}")
            print(f"Port: {result['connection_info'].get('port', 'Unknown')}")
            
            # Get tables
            tables = db.get_tables()
            print(f"Tables in database: {len(tables)}")
            if tables:
                print("Available tables:")
                for table in tables:
                    print(f"  - {table}")
            else:
                print("No tables found in the database.")
            db.close()
            return True
        else:
            print("[ERROR] Connection failed!")
            print(f"Error: {result['error']}")
            print(f"Connection info: {result['connection_info']}")
            db.close()
            return False
        
    except Exception as e:
        print(f"[ERROR] Connection test failed: {e}")
        return False

if __name__ == "__main__":
    test_database_connection()
