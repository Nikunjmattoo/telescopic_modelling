# Financial Data Downloaders

This directory contains scripts to download financial data for NSE-listed stocks.

## Available Downloaders

- **Income Statements** (`income_statement.py`)
  - Annual and quarterly income statements
  - Key metrics: Revenue, operating income, net income, EPS

- **Balance Sheets** (`balance_sheet.py`)
  - Annual and quarterly balance sheets
  - Key metrics: Assets, liabilities, equity, debt

- **Cash Flow** (`cash_flow.py`)
  - Annual and quarterly cash flow statements
  - Key metrics: Operating cash flow, free cash flow, dividends

- **Price History** (`price_history.py`)
  - Daily price and volume data
  - Includes adjusted close prices and dividends

- **Company Info** (`company_info.py`)
  - Company metadata
  - Sector, industry, market cap, etc.

## Usage

### Individual Downloaders

```bash
# Run a specific downloader
python downloaders/income_statement.py [--period annual|quarterly] [--force]
```

### Using the Main Script

```bash
# Run all downloaders
python downloaders/run_downloader.py all

# Run specific downloader
python downloaders/run_downloader.py income_statement --period annual

# Force re-download of existing files
python downloaders/run_downloader.py price_history --force
```

## Data Storage

Downloaded data is stored in the `data/` directory with the following structure:

```
data/
  ├── annual_income_statements/
  ├── quarterly_income_statements/
  ├── annual_balance_sheets/
  ├── quarterly_balance_sheets/
  ├── annual_cash_flow/
  ├── quarterly_cash_flow/
  ├── price_history/
  └── company_info/
```

## Notes

- **Rate Limiting**: Scripts include a 1-second delay between requests to avoid rate limiting
- **Error Handling**: Failed downloads are logged and can be retried
- **Incremental Updates**: By default, existing files are not re-downloaded (use `--force` to override)
