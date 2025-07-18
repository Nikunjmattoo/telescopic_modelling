-- Database schema for Telescopic Modelling Project
-- Created: 2025-07-14

-- Drop existing tables if they exist
DROP TABLE IF EXISTS corporate_action CASCADE;
DROP TABLE IF EXISTS price_history CASCADE;
DROP TABLE IF EXISTS cash_flow_quarterly CASCADE;
DROP TABLE IF EXISTS cash_flow_annual CASCADE;
DROP TABLE IF EXISTS income_statement_quarterly CASCADE;
DROP TABLE IF EXISTS income_statement_annual CASCADE;
DROP TABLE IF EXISTS balance_sheet_quarterly CASCADE;
DROP TABLE IF EXISTS balance_sheet_annual CASCADE;
DROP TABLE IF EXISTS ticker CASCADE;

-- 1. Master Ticker Table
CREATE TABLE ticker (
    ticker VARCHAR(20) PRIMARY KEY
);

-- 2. Balance Sheets
CREATE TABLE balance_sheet_annual (
    ticker VARCHAR(20) REFERENCES ticker(ticker),
    period_ending DATE,
    total_assets DECIMAL(25, 2),
    total_liabilities DECIMAL(25, 2),
    current_assets DECIMAL(25, 2),
    current_liabilities DECIMAL(25, 2),
    stockholders_equity DECIMAL(25, 2),
    total_debt DECIMAL(25, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, period_ending)
);

CREATE TABLE balance_sheet_quarterly (
    ticker VARCHAR(20) REFERENCES ticker(ticker),
    period_ending DATE,
    total_assets DECIMAL(25, 2),
    total_liabilities DECIMAL(25, 2),
    current_assets DECIMAL(25, 2),
    current_liabilities DECIMAL(25, 2),
    stockholders_equity DECIMAL(25, 2),
    total_debt DECIMAL(25, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, period_ending),
    CONSTRAINT valid_quarterly_date_bs CHECK (
        EXTRACT(DAY FROM period_ending) = 31 AND 
        EXTRACT(MONTH FROM period_ending) IN (3, 6, 9, 12)
    )
);

-- 3. Income Statements
CREATE TABLE income_statement_annual (
    ticker VARCHAR(20) REFERENCES ticker(ticker),
    period_ending DATE,
    total_revenue DECIMAL(25, 2),
    operating_income DECIMAL(25, 2),
    net_income DECIMAL(25, 2),
    basic_eps DECIMAL(15, 4),
    diluted_eps DECIMAL(15, 4),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, period_ending)
);

CREATE TABLE income_statement_quarterly (
    ticker VARCHAR(20) REFERENCES ticker(ticker),
    period_ending DATE,
    total_revenue DECIMAL(25, 2),
    operating_income DECIMAL(25, 2),
    net_income DECIMAL(25, 2),
    basic_eps DECIMAL(15, 4),
    diluted_eps DECIMAL(15, 4),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, period_ending),
    CONSTRAINT valid_quarterly_date_is CHECK (
        EXTRACT(DAY FROM period_ending) = 31 AND 
        EXTRACT(MONTH FROM period_ending) IN (3, 6, 9, 12)
    )
);

-- 4. Cash Flow
CREATE TABLE cash_flow_annual (
    ticker VARCHAR(20) REFERENCES ticker(ticker),
    period_ending DATE,
    operating_cash_flow DECIMAL(25, 2),
    free_cash_flow DECIMAL(25, 2),
    dividends_paid DECIMAL(25, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, period_ending)
);

CREATE TABLE cash_flow_quarterly (
    ticker VARCHAR(20) REFERENCES ticker(ticker),
    period_ending DATE,
    operating_cash_flow DECIMAL(25, 2),
    free_cash_flow DECIMAL(25, 2),
    dividends_paid DECIMAL(25, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, period_ending),
    CONSTRAINT valid_quarterly_date_cf CHECK (
        EXTRACT(DAY FROM period_ending) = 31 AND 
        EXTRACT(MONTH FROM period_ending) IN (3, 6, 9, 12)
    )
);

-- 5. Price History
CREATE TABLE price_history (
    ticker VARCHAR(20) REFERENCES ticker(ticker),
    date DATE,
    close_price DECIMAL(15, 4),
    adjusted_close_price DECIMAL(15, 4),
    volume BIGINT,
    dividends DECIMAL(15, 4) DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date)
);

-- 6. Corporate Actions
CREATE TABLE corporate_action (
    ticker VARCHAR(20) REFERENCES ticker(ticker),
    action_date DATE,
    action_type VARCHAR(20),  -- 'SPLIT', 'BONUS', 'DIVIDEND'
    details JSONB,  -- Flexible field for different action types
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, action_date, action_type)
);

-- Add indexes for better query performance
CREATE INDEX idx_price_history_ticker ON price_history(ticker);
CREATE INDEX idx_price_history_date ON price_history(date);
CREATE INDEX idx_balance_sheet_annual_ticker ON balance_sheet_annual(ticker);
CREATE INDEX idx_income_statement_annual_ticker ON income_statement_annual(ticker);
CREATE INDEX idx_cash_flow_annual_ticker ON cash_flow_annual(ticker);

-- Function to update last_updated timestamp
CREATE OR REPLACE FUNCTION update_last_updated()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_updated = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for each table to update last_updated
DO $$
DECLARE
    t text;
BEGIN
    FOR t IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name IN (
            'balance_sheet_annual',
            'balance_sheet_quarterly',
            'income_statement_annual',
            'income_statement_quarterly',
            'cash_flow_annual',
            'cash_flow_quarterly',
            'price_history',
            'corporate_action'
        )
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS update_%s_timestamp ON %I', t, t);
        EXECUTE format('CREATE TRIGGER update_%s_timestamp
                      BEFORE UPDATE ON %I
                      FOR EACH ROW EXECUTE FUNCTION update_last_updated()', t, t);
    END LOOP;
END;
$$;
