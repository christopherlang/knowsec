CREATE TABLE public.securities
(
    secid VARCHAR(100) NOT NULL PRIMARY KEY,
    company_id VARCHAR(100),
    name VARCHAR(250),
    type VARCHAR(100),
    code VARCHAR(100),
    share_class VARCHAR(100),
    currency VARCHAR(100),
    round_lot_size DECIMAL(20, 6),
    ticker VARCHAR(100),
    exchange_ticker VARCHAR(100),
    composite_ticker VARCHAR(100),
    alternate_tickers VARCHAR(100)[],
    figi VARCHAR(100),
    cik VARCHAR(100),
    composite_figi VARCHAR(100),
    share_class_figi VARCHAR(100),
    figi_uniqueid VARCHAR(100),
    active BOOLEAN,
    etf BOOLEAN,
    delisted BOOLEAN,
    primary_listing BOOLEAN,
    primary_security BOOLEAN,
    first_stock_price date,
    last_stock_price date,
    last_stock_price_adjustment date,
    last_corporate_action date,
    previous_tickers VARCHAR(100)[],
    listing_exchange_mic VARCHAR(100)
)



CREATE TABLE public.exchanges
(
    EXCID VARCHAR(100) NOT NULL,
    mic VARCHAR(250) NOT NULL,
    acronym VARCHAR(250),
    name VARCHAR(250),
    country VARCHAR(250),
    country_code VARCHAR(250),
    city VARCHAR(250),
    website VARCHAR(1000),
    first_stock_price_date date,
    last_stock_price_date date,
    PRIMARY KEY (EXCID, mic)
)


CREATE TABLE public.security_prices
(
    secid VARCHAR(50) NOT NULL REFERENCES securities(secid) ON DELETE RESTRICT,
    date DATE NOT NULL,
    frequency VARCHAR(50) NOT NULL,
    intraperiod BOOLEAN NOT NULL,
    open DECIMAL(20, 6),
    low DECIMAL(20, 6),
    high DECIMAL(20, 6),
    close DECIMAL(20, 6),
    volume BIGINT,
    adj_open DECIMAL(100, 20),
    adj_low DECIMAL(100, 20),
    adj_high DECIMAL(100, 20),
    adj_close DECIMAL(100, 20),
    adj_volume DECIMAL(100, 20),
    PRIMARY KEY (secid, date, frequency, intraperiod)
)

CREATE TABLE public.update_log
(
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    start_datetime TIMESTAMPTZ NOT NULL,
    end_datetime TIMESTAMPTZ NOT NULL,
    elapsed_seconds DECIMAL(50, 4) NOT NULL,
    num_api_queries INT,
    num_api_requests INT,
    num_new_records INT,
    num_update_records INT,
    num_insert_records INT
)

-- Code for droping and recreating the prices_log table
DROP TABLE public.prices_log;

CREATE TABLE public.prices_log
(
    secid VARCHAR(50) NOT NULL PRIMARY KEY REFERENCES securities(secid) ON DELETE RESTRICT,
    min_date DATE NOT NULL,
    max_date DATE NOT NULL,
    update_dt TIMESTAMPTZ,
    check_dt TIMESTAMPTZ
);

INSERT INTO prices_log (ticker, min_date, max_date)
SELECT ticker,
       MIN(date) as min_date,
       MAX(date) as max_date
    FROM security_prices
    GROUP BY ticker;
