-- ============================================================================
-- ABC Hub Staging Database Schema
-- Module: DWDF Mini Project 01 - Capstone Project
-- Schema: staging
-- Engine: PostgreSQL 14+
-- Description: Staging tables for landing cleansed operational data from NiFi
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
SET search_path TO staging, public;

-- Drop staging tables if re-initializing
DROP TABLE IF EXISTS staging.wishlist CASCADE;
DROP TABLE IF EXISTS staging.review CASCADE;
DROP TABLE IF EXISTS staging.support_ticket CASCADE;
DROP TABLE IF EXISTS staging.payment CASCADE;
DROP TABLE IF EXISTS staging.rental CASCADE;
DROP TABLE IF EXISTS staging.streaming_session CASCADE;
DROP TABLE IF EXISTS staging.inventory_item CASCADE;
DROP TABLE IF EXISTS staging.warehouse CASCADE;
DROP TABLE IF EXISTS staging.content_genre CASCADE;
DROP TABLE IF EXISTS staging.genre CASCADE;
DROP TABLE IF EXISTS staging.content_type CASCADE;
DROP TABLE IF EXISTS staging.content CASCADE;
DROP TABLE IF EXISTS staging.customer_subscription CASCADE;
DROP TABLE IF EXISTS staging.subscription_plan CASCADE;
DROP TABLE IF EXISTS staging.customer_address CASCADE;
DROP TABLE IF EXISTS staging.city CASCADE;
DROP TABLE IF EXISTS staging.country CASCADE;
DROP TABLE IF EXISTS staging.customer CASCADE;

-- ----------------------------------------------------------------------------
-- 1. Reference & Geography
-- ----------------------------------------------------------------------------
CREATE TABLE staging.country (
    country_id      BIGINT PRIMARY KEY,
    country_name    VARCHAR(100),
    country_code    VARCHAR(5),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

CREATE TABLE staging.city (
    city_id         BIGINT PRIMARY KEY,
    country_id      BIGINT,
    city_name       VARCHAR(100),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 2. Customer Domain
-- ----------------------------------------------------------------------------
CREATE TABLE staging.customer (
    customer_id       BIGINT PRIMARY KEY,
    customer_no       VARCHAR(20),
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    email             VARCHAR(150),
    phone             VARCHAR(30),
    date_of_birth     VARCHAR(50),
    gender            VARCHAR(20),
    registration_date VARCHAR(50),
    status            VARCHAR(20),
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP
);

CREATE TABLE staging.customer_address (
    address_id      BIGINT PRIMARY KEY,
    customer_id     BIGINT,
    city_id         BIGINT,
    address_line    VARCHAR(255),
    postal_code     VARCHAR(20),
    address_type    VARCHAR(20),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

CREATE TABLE staging.subscription_plan (
    plan_id         BIGINT PRIMARY KEY,
    plan_name       VARCHAR(50),
    monthly_fee     DECIMAL(10,2),
    video_quality   VARCHAR(20),
    max_devices     INT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

CREATE TABLE staging.customer_subscription (
    subscription_id BIGINT PRIMARY KEY,
    customer_id     BIGINT,
    plan_id         BIGINT,
    start_date      VARCHAR(50),
    end_date        VARCHAR(50),
    status          VARCHAR(20),
    auto_renew      BOOLEAN,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 3. Content Domain
-- ----------------------------------------------------------------------------
CREATE TABLE staging.content_type (
    content_type_id BIGINT PRIMARY KEY,
    content_type    VARCHAR(50),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

CREATE TABLE staging.genre (
    genre_id        BIGINT PRIMARY KEY,
    genre_name      VARCHAR(50),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

CREATE TABLE staging.content (
    content_id       BIGINT PRIMARY KEY,
    content_type_id  BIGINT,
    title            VARCHAR(255),
    release_date     VARCHAR(50),
    duration_minutes INT,
    language         VARCHAR(50),
    age_rating       VARCHAR(10),
    created_at       TIMESTAMP,
    updated_at       TIMESTAMP
);

CREATE TABLE staging.content_genre (
    content_id      BIGINT,
    genre_id        BIGINT,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    PRIMARY KEY (content_id, genre_id)
);

-- ----------------------------------------------------------------------------
-- 4. Inventory & Warehouse
-- ----------------------------------------------------------------------------
CREATE TABLE staging.warehouse (
    warehouse_id    BIGINT PRIMARY KEY,
    city_id         BIGINT,
    warehouse_name  VARCHAR(150),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

CREATE TABLE staging.inventory_item (
    inventory_id    BIGINT PRIMARY KEY,
    content_id      BIGINT,
    warehouse_id    BIGINT,
    barcode         VARCHAR(50),
    purchase_date   VARCHAR(50),
    item_condition  VARCHAR(20),
    status          VARCHAR(20),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 5. Transactions & Engagement
-- ----------------------------------------------------------------------------
CREATE TABLE staging.rental (
    rental_id       BIGINT PRIMARY KEY,
    customer_id     BIGINT,
    inventory_id    BIGINT,
    rental_date     VARCHAR(50),
    due_date        VARCHAR(50),
    return_date     VARCHAR(50),
    rental_fee      DECIMAL(10,2),
    late_fee        DECIMAL(10,2),
    status          VARCHAR(20),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

CREATE TABLE staging.streaming_session (
    stream_id             BIGINT PRIMARY KEY,
    customer_id           BIGINT,
    content_id            BIGINT,
    device_id             BIGINT,
    start_time            TIMESTAMP,
    end_time              TIMESTAMP,
    watch_duration        INT,
    completion_percentage DECIMAL(5,2),
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP
);

CREATE TABLE staging.payment (
    payment_id        BIGINT PRIMARY KEY,
    customer_id       BIGINT,
    rental_id         BIGINT,
    subscription_id   BIGINT,
    payment_method_id BIGINT,
    amount            DECIMAL(10,2),
    payment_date      TIMESTAMP,
    payment_type      VARCHAR(20),
    status            VARCHAR(20),
    created_at        TIMESTAMP,
    updated_at        TIMESTAMP
);

CREATE TABLE staging.support_ticket (
    ticket_id       BIGINT PRIMARY KEY,
    customer_id     BIGINT,
    category_id     BIGINT,
    opened_date     TIMESTAMP,
    closed_date     TIMESTAMP,
    priority        VARCHAR(20),
    status          VARCHAR(20),
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

CREATE TABLE staging.review (
    review_id       BIGINT PRIMARY KEY,
    customer_id     BIGINT,
    content_id      BIGINT,
    rating          INT,
    review_text     VARCHAR(500),
    review_date     TIMESTAMP,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

CREATE TABLE staging.wishlist (
    wishlist_id     BIGINT PRIMARY KEY,
    customer_id     BIGINT,
    content_id      BIGINT,
    added_date      TIMESTAMP,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP
);

