-- ============================================================================
-- ABC Hub Analytical Data Warehouse - Schema Definition
-- Module: DWDF Mini Project 01 - Capstone Project
-- Schema: analytical
-- Engine: PostgreSQL 14+
-- Description: DDL for Analytical Star Schema Dimension and Fact Tables
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS analytical;
SET search_path TO analytical, public;

-- ----------------------------------------------------------------------------
-- 1. Conformed Dimension: dim_date
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytical.dim_date (
    date_key          INT PRIMARY KEY,              -- Format: YYYYMMDD
    full_date         DATE NOT NULL UNIQUE,
    day_of_month      INT NOT NULL,
    day_name          VARCHAR(15) NOT NULL,
    day_of_week       INT NOT NULL,                 -- 1 = Monday, 7 = Sunday
    month_number      INT NOT NULL,
    month_name        VARCHAR(20) NOT NULL,
    calendar_quarter  INT NOT NULL,
    calendar_year     INT NOT NULL,
    is_weekend        BOOLEAN NOT NULL
);

-- ----------------------------------------------------------------------------
-- 2. Dimension: dim_customer (Denormalized)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytical.dim_customer (
    customer_key      SERIAL PRIMARY KEY,
    customer_id       BIGINT NOT NULL UNIQUE,       -- Operational Natural Key
    customer_no       VARCHAR(20) NOT NULL,
    customer_name     VARCHAR(200),
    gender            VARCHAR(20),
    city_name         VARCHAR(100),
    country_name      VARCHAR(100),
    customer_segment  VARCHAR(50),
    status            VARCHAR(20),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 3. Dimension: dim_content (Denormalized)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytical.dim_content (
    content_key       SERIAL PRIMARY KEY,
    content_id        BIGINT NOT NULL UNIQUE,       -- Operational Natural Key
    content_title     VARCHAR(255) NOT NULL,
    content_type      VARCHAR(50),
    primary_genre     VARCHAR(50),
    all_genres        VARCHAR(255),
    release_date      DATE,
    language          VARCHAR(50),
    age_rating        VARCHAR(10),
    duration_minutes  INT,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 4. Dimension: dim_inventory_item (Denormalized)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytical.dim_inventory_item (
    inventory_key     SERIAL PRIMARY KEY,
    inventory_id      BIGINT NOT NULL UNIQUE,       -- Operational Natural Key
    content_id        BIGINT NOT NULL,
    content_title     VARCHAR(255),
    barcode           VARCHAR(50),
    purchase_date     DATE,
    item_condition    VARCHAR(20),
    warehouse_id      BIGINT,
    warehouse_name    VARCHAR(150),
    warehouse_city    VARCHAR(100),
    item_status       VARCHAR(20),
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 5. Fact Table A: fact_customer_daily_activity (Scenario A)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytical.fact_customer_daily_activity (
    customer_key                  INT NOT NULL REFERENCES analytical.dim_customer(customer_key),
    date_key                      INT NOT NULL REFERENCES analytical.dim_date(date_key),
    total_streaming_sessions      INT DEFAULT 0,
    total_streaming_duration_mins INT DEFAULT 0,
    total_physical_rentals        INT DEFAULT 0,
    total_returned_items          INT DEFAULT 0,
    total_amount_spent            NUMERIC(10,2) DEFAULT 0.00,
    total_support_tickets         INT DEFAULT 0,
    loaded_at                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_key, date_key)
);

-- ----------------------------------------------------------------------------
-- 6. Fact Table B: fact_content_monthly_performance (Scenario B)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytical.fact_content_monthly_performance (
    content_key                   INT NOT NULL REFERENCES analytical.dim_content(content_key),
    month_date_key                INT NOT NULL REFERENCES analytical.dim_date(date_key),
    total_streams                 INT DEFAULT 0,
    total_rental_count            INT DEFAULT 0,
    total_revenue                 NUMERIC(12,2) DEFAULT 0.00,
    total_rating_score            INT DEFAULT 0,
    total_review_count            INT DEFAULT 0,
    average_customer_rating       NUMERIC(3,2) DEFAULT 0.00,
    total_wishlist_additions      INT DEFAULT 0,
    loaded_at                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (content_key, month_date_key)
);

-- ----------------------------------------------------------------------------
-- 7. Fact Table C: fact_inventory_daily_utilisation (Scenario C)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytical.fact_inventory_daily_utilisation (
    inventory_key                 INT NOT NULL REFERENCES analytical.dim_inventory_item(inventory_key),
    date_key                      INT NOT NULL REFERENCES analytical.dim_date(date_key),
    total_rentals                 INT DEFAULT 0,
    total_returns                 INT DEFAULT 0,
    available_day_count           INT DEFAULT 0,
    rented_day_count              INT DEFAULT 0,
    utilisation_percentage        NUMERIC(5,2) DEFAULT 0.00,
    loaded_at                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (inventory_key, date_key)
);

-- ----------------------------------------------------------------------------
-- 8. Performance Indexes
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_fact_cust_date ON analytical.fact_customer_daily_activity(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_content_date ON analytical.fact_content_monthly_performance(month_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_inv_date ON analytical.fact_inventory_daily_utilisation(date_key);

