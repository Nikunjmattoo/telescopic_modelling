# Database Setup Guide - Telescopic Modelling Project

## Current Status
The database connection is failing with authentication errors. This suggests the previous database may have been deleted or credentials changed.

## Quick Setup Options

### Option 1: Create New Neon Database (Recommended)
1. Go to [Neon Console](https://console.neon.tech/)
2. Create a new project called "telescopic-modelling"
3. Copy the connection string
4. Update the `DATABASE_URL` in `.env` file

### Option 2: Use Local PostgreSQL
If you have PostgreSQL installed locally:
1. Create a new database: `createdb telescopic_modelling`
2. Update `.env` with: `DATABASE_URL=postgresql://username:password@localhost:5432/telescopic_modelling`

### Option 3: Use SQLite (Temporary)
For quick testing, we can switch to SQLite temporarily:
1. Update requirements.txt to remove psycopg
2. Use sqlite3 (built into Python)

## Files Ready
- ✅ `db_utils.py` - Database utility class (updated for psycopg3)
- ✅ `test_connection.py` - Connection test script
- ✅ `requirements.txt` - Dependencies installed
- ✅ `.env` - Environment configuration (needs valid DATABASE_URL)

## Next Steps
1. Set up a working database (choose option above)
2. Update DATABASE_URL in `.env` file
3. Run `python test_connection.py` to verify connection
4. Proceed with CSV download script development

## Current Error
```
ERROR: password authentication failed for user 'telescopic_owner'
```

This indicates the database credentials are invalid or the database no longer exists.
