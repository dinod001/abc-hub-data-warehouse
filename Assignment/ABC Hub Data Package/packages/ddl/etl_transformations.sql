
-- ----------------------------------------------------------------------------
-- 0. Helper Function: Robust Date Parser for Staging Data
-- Handles both Unix Epoch Milliseconds ('1753900200000') and standard ISO ('2025-07-31')
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytical.fn_parse_date(d_val TEXT)
RETURNS DATE
LANGUAGE sql IMMUTABLE
AS $$
    SELECT CASE 
        WHEN d_val IS NULL OR TRIM(d_val) = '' THEN NULL
        WHEN d_val ~ '^[0-9]+$' THEN to_timestamp(d_val::bigint / 1000.0)::DATE
        ELSE d_val::DATE
    END;
$$;

-- ============================================================================
-- ABC Hub Analytical Data Warehouse ETL Transformations
-- Module: DWDF Mini Project 01 - Capstone Project
-- Schema: analytical
-- Engine: PostgreSQL 14+
-- Description: Transforms and loads data from staging schema into analytical Star Schema
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Helper Function: Populate Conformed Date Dimension (2020 to 2030)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE analytical.sp_populate_dim_date(start_year INT, end_year INT)
LANGUAGE plpgsql
AS $$
DECLARE
    cur_date DATE;
    end_date DATE;
BEGIN
    cur_date := (start_year || '-01-01')::DATE;
    end_date := (end_year || '-12-31')::DATE;

    WHILE cur_date <= end_date LOOP
        INSERT INTO analytical.dim_date (
            date_key,
            full_date,
            day_of_month,
            day_name,
            day_of_week,
            month_number,
            month_name,
            calendar_quarter,
            calendar_year,
            is_weekend
        )
        VALUES (
            TO_CHAR(cur_date, 'YYYYMMDD')::INT,
            cur_date,
            EXTRACT(DAY FROM cur_date)::INT,
            TRIM(TO_CHAR(cur_date, 'Day')),
            EXTRACT(ISODOW FROM cur_date)::INT,
            EXTRACT(MONTH FROM cur_date)::INT,
            TRIM(TO_CHAR(cur_date, 'Month')),
            EXTRACT(QUARTER FROM cur_date)::INT,
            EXTRACT(YEAR FROM cur_date)::INT,
            CASE WHEN EXTRACT(ISODOW FROM cur_date) IN (6, 7) THEN TRUE ELSE FALSE END
        )
        ON CONFLICT (date_key) DO NOTHING;

        cur_date := cur_date + INTERVAL '1 day';
    END LOOP;
END;
$$;

-- Populate dim_date from 2020 to 2030
CALL analytical.sp_populate_dim_date(2020, 2030);


-- ----------------------------------------------------------------------------
-- 2. Master ETL Stored Procedure: Runs full dimension & fact transformations
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE analytical.sp_run_full_etl()
LANGUAGE plpgsql
AS $$
BEGIN
    -- ------------------------------------------------------------------------
    -- STEP 1: Load Dimension: dim_customer (Denormalized)
    -- ------------------------------------------------------------------------
    RAISE NOTICE 'Loading analytical.dim_customer...';
    
    INSERT INTO analytical.dim_customer (
        customer_id,
        customer_no,
        customer_name,
        gender,
        city_name,
        country_name,
        customer_segment,
        status
    )
    SELECT 
        c.customer_id,
        c.customer_no,
        TRIM(CONCAT(c.first_name, ' ', c.last_name)) AS customer_name,
        COALESCE(c.gender, 'Unknown') AS gender,
        COALESCE(ci.city_name, 'Unknown') AS city_name,
        COALESCE(co.country_name, 'Unknown') AS country_name,
        COALESCE(sp.plan_name, 'Standard') AS customer_segment,
        COALESCE(c.status, 'Active') AS status
    FROM staging.customer c
    LEFT JOIN (
        -- Pick primary or latest address per customer
        SELECT DISTINCT ON (customer_id) customer_id, city_id
        FROM staging.customer_address
        ORDER BY customer_id, address_id DESC
    ) ca ON c.customer_id = ca.customer_id
    LEFT JOIN staging.city ci ON ca.city_id = ci.city_id
    LEFT JOIN staging.country co ON ci.country_id = co.country_id
    LEFT JOIN (
        -- Pick active or latest subscription per customer
        SELECT DISTINCT ON (customer_id) customer_id, plan_id
        FROM staging.customer_subscription
        ORDER BY customer_id, subscription_id DESC
    ) cs ON c.customer_id = cs.customer_id
    LEFT JOIN staging.subscription_plan sp ON cs.plan_id = sp.plan_id
    ON CONFLICT (customer_id) DO UPDATE SET
        customer_no = EXCLUDED.customer_no,
        customer_name = EXCLUDED.customer_name,
        gender = EXCLUDED.gender,
        city_name = EXCLUDED.city_name,
        country_name = EXCLUDED.country_name,
        customer_segment = EXCLUDED.customer_segment,
        status = EXCLUDED.status;


    -- ------------------------------------------------------------------------
    -- STEP 2: Load Dimension: dim_content (Denormalized)
    -- ------------------------------------------------------------------------
    RAISE NOTICE 'Loading analytical.dim_content...';

    INSERT INTO analytical.dim_content (
        content_id,
        content_title,
        content_type,
        primary_genre,
        all_genres,
        release_date,
        language,
        age_rating,
        duration_minutes
    )
    SELECT 
        c.content_id,
        TRIM(c.title) AS content_title,
        COALESCE(ct.content_type, 'Movie') AS content_type,
        COALESCE(g_info.primary_genre, 'General') AS primary_genre,
        COALESCE(g_info.all_genres, 'General') AS all_genres,
        analytical.fn_parse_date(c.release_date) AS release_date,
        COALESCE(c.language, 'English') AS language,
        COALESCE(c.age_rating, 'NR') AS age_rating,
        c.duration_minutes
    FROM staging.content c
    LEFT JOIN staging.content_type ct ON c.content_type_id = ct.content_type_id
    LEFT JOIN (
        -- Aggregate genres and extract first genre as primary
        SELECT 
            cg.content_id,
            STRING_AGG(g.genre_name, ', ' ORDER BY g.genre_name) AS all_genres,
            (ARRAY_AGG(g.genre_name ORDER BY g.genre_id))[1] AS primary_genre
        FROM staging.content_genre cg
        JOIN staging.genre g ON cg.genre_id = g.genre_id
        GROUP BY cg.content_id
    ) g_info ON c.content_id = g_info.content_id
    ON CONFLICT (content_id) DO UPDATE SET
        content_title = EXCLUDED.content_title,
        content_type = EXCLUDED.content_type,
        primary_genre = EXCLUDED.primary_genre,
        all_genres = EXCLUDED.all_genres,
        release_date = EXCLUDED.release_date,
        language = EXCLUDED.language,
        age_rating = EXCLUDED.age_rating,
        duration_minutes = EXCLUDED.duration_minutes;


    -- ------------------------------------------------------------------------
    -- STEP 3: Load Dimension: dim_inventory_item (Denormalized)
    -- ------------------------------------------------------------------------
    RAISE NOTICE 'Loading analytical.dim_inventory_item...';

    INSERT INTO analytical.dim_inventory_item (
        inventory_id,
        content_id,
        content_title,
        barcode,
        purchase_date,
        item_condition,
        warehouse_id,
        warehouse_name,
        warehouse_city,
        item_status
    )
    SELECT 
        i.inventory_id,
        i.content_id,
        COALESCE(c.title, 'Unknown Title') AS content_title,
        i.barcode,
        analytical.fn_parse_date(i.purchase_date) AS purchase_date,
        COALESCE(i.item_condition, 'Good') AS item_condition,
        i.warehouse_id,
        COALESCE(w.warehouse_name, 'Main Warehouse') AS warehouse_name,
        COALESCE(ci.city_name, 'Unknown') AS warehouse_city,
        COALESCE(i.status, 'Available') AS item_status
    FROM staging.inventory_item i
    LEFT JOIN staging.content c ON i.content_id = c.content_id
    LEFT JOIN staging.warehouse w ON i.warehouse_id = w.warehouse_id
    LEFT JOIN staging.city ci ON w.city_id = ci.city_id
    ON CONFLICT (inventory_id) DO UPDATE SET
        content_id = EXCLUDED.content_id,
        content_title = EXCLUDED.content_title,
        barcode = EXCLUDED.barcode,
        purchase_date = EXCLUDED.purchase_date,
        item_condition = EXCLUDED.item_condition,
        warehouse_id = EXCLUDED.warehouse_id,
        warehouse_name = EXCLUDED.warehouse_name,
        warehouse_city = EXCLUDED.warehouse_city,
        item_status = EXCLUDED.item_status;


    -- ------------------------------------------------------------------------
    -- STEP 4: Load Fact Table A: fact_customer_daily_activity (Scenario A)
    -- Grain: One record per customer per calendar day
    -- ------------------------------------------------------------------------
    RAISE NOTICE 'Loading analytical.fact_customer_daily_activity...';

    WITH daily_streams AS (
        SELECT 
            customer_id, 
            TO_CHAR(start_time, 'YYYYMMDD')::INT AS date_key,
            COUNT(stream_id) AS total_streaming_sessions,
            COALESCE(SUM(watch_duration), 0) AS total_streaming_duration_mins
        FROM staging.streaming_session
        WHERE start_time IS NOT NULL
        GROUP BY customer_id, TO_CHAR(start_time, 'YYYYMMDD')::INT
    ),
    daily_rentals AS (
        SELECT 
            customer_id, 
            TO_CHAR(analytical.fn_parse_date(rental_date), 'YYYYMMDD')::INT AS date_key,
            COUNT(rental_id) AS total_physical_rentals
        FROM staging.rental
        WHERE analytical.fn_parse_date(rental_date) IS NOT NULL
        GROUP BY customer_id, TO_CHAR(analytical.fn_parse_date(rental_date), 'YYYYMMDD')::INT
    ),
    daily_returns AS (
        SELECT 
            customer_id, 
            TO_CHAR(analytical.fn_parse_date(return_date), 'YYYYMMDD')::INT AS date_key,
            COUNT(rental_id) AS total_returned_items
        FROM staging.rental
        WHERE analytical.fn_parse_date(return_date) IS NOT NULL
        GROUP BY customer_id, TO_CHAR(analytical.fn_parse_date(return_date), 'YYYYMMDD')::INT
    ),
    daily_payments AS (
        SELECT 
            customer_id, 
            TO_CHAR(payment_date, 'YYYYMMDD')::INT AS date_key,
            COALESCE(SUM(amount), 0.00) AS total_amount_spent
        FROM staging.payment
        WHERE payment_date IS NOT NULL
        GROUP BY customer_id, TO_CHAR(payment_date, 'YYYYMMDD')::INT
    ),
    daily_tickets AS (
        SELECT 
            customer_id, 
            TO_CHAR(opened_date, 'YYYYMMDD')::INT AS date_key,
            COUNT(ticket_id) AS total_support_tickets
        FROM staging.support_ticket
        WHERE opened_date IS NOT NULL
        GROUP BY customer_id, TO_CHAR(opened_date, 'YYYYMMDD')::INT
    ),
    all_customer_days AS (
        SELECT customer_id, date_key FROM daily_streams
        UNION
        SELECT customer_id, date_key FROM daily_rentals
        UNION
        SELECT customer_id, date_key FROM daily_returns
        UNION
        SELECT customer_id, date_key FROM daily_payments
        UNION
        SELECT customer_id, date_key FROM daily_tickets
    )
    INSERT INTO analytical.fact_customer_daily_activity (
        customer_key,
        date_key,
        total_streaming_sessions,
        total_streaming_duration_mins,
        total_physical_rentals,
        total_returned_items,
        total_amount_spent,
        total_support_tickets,
        loaded_at
    )
    SELECT 
        dc.customer_key,
        acd.date_key,
        COALESCE(ds.total_streaming_sessions, 0),
        COALESCE(ds.total_streaming_duration_mins, 0),
        COALESCE(dr.total_physical_rentals, 0),
        COALESCE(dret.total_returned_items, 0),
        COALESCE(dp.total_amount_spent, 0.00),
        COALESCE(dt.total_support_tickets, 0),
        CURRENT_TIMESTAMP
    FROM all_customer_days acd
    JOIN analytical.dim_customer dc ON acd.customer_id = dc.customer_id
    JOIN analytical.dim_date dd ON acd.date_key = dd.date_key
    LEFT JOIN daily_streams ds ON acd.customer_id = ds.customer_id AND acd.date_key = ds.date_key
    LEFT JOIN daily_rentals dr ON acd.customer_id = dr.customer_id AND acd.date_key = dr.date_key
    LEFT JOIN daily_returns dret ON acd.customer_id = dret.customer_id AND acd.date_key = dret.date_key
    LEFT JOIN daily_payments dp ON acd.customer_id = dp.customer_id AND acd.date_key = dp.date_key
    LEFT JOIN daily_tickets dt ON acd.customer_id = dt.customer_id AND acd.date_key = dt.date_key
    ON CONFLICT (customer_key, date_key) DO UPDATE SET
        total_streaming_sessions = EXCLUDED.total_streaming_sessions,
        total_streaming_duration_mins = EXCLUDED.total_streaming_duration_mins,
        total_physical_rentals = EXCLUDED.total_physical_rentals,
        total_returned_items = EXCLUDED.total_returned_items,
        total_amount_spent = EXCLUDED.total_amount_spent,
        total_support_tickets = EXCLUDED.total_support_tickets,
        loaded_at = CURRENT_TIMESTAMP;


    -- ------------------------------------------------------------------------
    -- STEP 5: Load Fact Table B: fact_content_monthly_performance (Scenario B)
    -- Grain: One record per content item per calendar month
    -- ------------------------------------------------------------------------
    RAISE NOTICE 'Loading analytical.fact_content_monthly_performance...';

    WITH monthly_streams AS (
        SELECT 
            content_id,
            (TO_CHAR(start_time, 'YYYYMM') || '01')::INT AS month_date_key,
            COUNT(stream_id) AS total_streams
        FROM staging.streaming_session
        WHERE start_time IS NOT NULL
        GROUP BY content_id, (TO_CHAR(start_time, 'YYYYMM') || '01')::INT
    ),
    monthly_rentals AS (
        SELECT 
            ii.content_id,
            (TO_CHAR(analytical.fn_parse_date(r.rental_date), 'YYYYMM') || '01')::INT AS month_date_key,
            COUNT(r.rental_id) AS total_rental_count,
            COALESCE(SUM(r.rental_fee + COALESCE(r.late_fee, 0.00)), 0.00) AS total_revenue
        FROM staging.rental r
        JOIN staging.inventory_item ii ON r.inventory_id = ii.inventory_id
        WHERE analytical.fn_parse_date(r.rental_date) IS NOT NULL
        GROUP BY ii.content_id, (TO_CHAR(analytical.fn_parse_date(r.rental_date), 'YYYYMM') || '01')::INT
    ),
    monthly_reviews AS (
        SELECT 
            content_id,
            (TO_CHAR(review_date, 'YYYYMM') || '01')::INT AS month_date_key,
            COALESCE(SUM(rating), 0) AS total_rating_score,
            COUNT(review_id) AS total_review_count,
            ROUND(AVG(rating), 2) AS average_customer_rating
        FROM staging.review
        WHERE review_date IS NOT NULL
        GROUP BY content_id, (TO_CHAR(review_date, 'YYYYMM') || '01')::INT
    ),
    monthly_wishlists AS (
        SELECT 
            content_id,
            (TO_CHAR(added_date, 'YYYYMM') || '01')::INT AS month_date_key,
            COUNT(wishlist_id) AS total_wishlist_additions
        FROM staging.wishlist
        WHERE added_date IS NOT NULL
        GROUP BY content_id, (TO_CHAR(added_date, 'YYYYMM') || '01')::INT
    ),
    all_content_months AS (
        SELECT content_id, month_date_key FROM monthly_streams
        UNION
        SELECT content_id, month_date_key FROM monthly_rentals
        UNION
        SELECT content_id, month_date_key FROM monthly_reviews
        UNION
        SELECT content_id, month_date_key FROM monthly_wishlists
    )
    INSERT INTO analytical.fact_content_monthly_performance (
        content_key,
        month_date_key,
        total_streams,
        total_rental_count,
        total_revenue,
        total_rating_score,
        total_review_count,
        average_customer_rating,
        total_wishlist_additions,
        loaded_at
    )
    SELECT 
        dc.content_key,
        acm.month_date_key,
        COALESCE(ms.total_streams, 0),
        COALESCE(mr.total_rental_count, 0),
        COALESCE(mr.total_revenue, 0.00),
        COALESCE(mrev.total_rating_score, 0),
        COALESCE(mrev.total_review_count, 0),
        COALESCE(mrev.average_customer_rating, 0.00),
        COALESCE(mw.total_wishlist_additions, 0),
        CURRENT_TIMESTAMP
    FROM all_content_months acm
    JOIN analytical.dim_content dc ON acm.content_id = dc.content_id
    JOIN analytical.dim_date dd ON acm.month_date_key = dd.date_key
    LEFT JOIN monthly_streams ms ON acm.content_id = ms.content_id AND acm.month_date_key = ms.month_date_key
    LEFT JOIN monthly_rentals mr ON acm.content_id = mr.content_id AND acm.month_date_key = mr.month_date_key
    LEFT JOIN monthly_reviews mrev ON acm.content_id = mrev.content_id AND acm.month_date_key = mrev.month_date_key
    LEFT JOIN monthly_wishlists mw ON acm.content_id = mw.content_id AND acm.month_date_key = mw.month_date_key
    ON CONFLICT (content_key, month_date_key) DO UPDATE SET
        total_streams = EXCLUDED.total_streams,
        total_rental_count = EXCLUDED.total_rental_count,
        total_revenue = EXCLUDED.total_revenue,
        total_rating_score = EXCLUDED.total_rating_score,
        total_review_count = EXCLUDED.total_review_count,
        average_customer_rating = EXCLUDED.average_customer_rating,
        total_wishlist_additions = EXCLUDED.total_wishlist_additions,
        loaded_at = CURRENT_TIMESTAMP;


    -- ------------------------------------------------------------------------
    -- STEP 6: Load Fact Table C: fact_inventory_daily_utilisation (Scenario C)
    -- Grain: One record per physical inventory item per calendar day
    -- ------------------------------------------------------------------------
    RAISE NOTICE 'Loading analytical.fact_inventory_daily_utilisation...';

    WITH daily_item_rentals AS (
        SELECT 
            inventory_id,
            TO_CHAR(analytical.fn_parse_date(rental_date), 'YYYYMMDD')::INT AS date_key,
            COUNT(rental_id) AS total_rentals
        FROM staging.rental
        WHERE analytical.fn_parse_date(rental_date) IS NOT NULL
        GROUP BY inventory_id, TO_CHAR(analytical.fn_parse_date(rental_date), 'YYYYMMDD')::INT
    ),
    daily_item_returns AS (
        SELECT 
            inventory_id,
            TO_CHAR(analytical.fn_parse_date(return_date), 'YYYYMMDD')::INT AS date_key,
            COUNT(rental_id) AS total_returns
        FROM staging.rental
        WHERE analytical.fn_parse_date(return_date) IS NOT NULL
        GROUP BY inventory_id, TO_CHAR(analytical.fn_parse_date(return_date), 'YYYYMMDD')::INT
    ),
    -- Active rented days per item (spans from rental_date to return_date)
    active_rented_days AS (
        SELECT 
            r.inventory_id,
            dd.date_key,
            1 AS rented_day_count
        FROM staging.rental r
        JOIN analytical.dim_date dd 
          ON dd.full_date >= analytical.fn_parse_date(r.rental_date) 
         AND dd.full_date <= COALESCE(analytical.fn_parse_date(r.return_date), CURRENT_DATE)
        GROUP BY r.inventory_id, dd.date_key
    ),
    all_inv_days AS (
        SELECT inventory_id, date_key FROM daily_item_rentals
        UNION
        SELECT inventory_id, date_key FROM daily_item_returns
        UNION
        SELECT inventory_id, date_key FROM active_rented_days
    )
    INSERT INTO analytical.fact_inventory_daily_utilisation (
        inventory_key,
        date_key,
        total_rentals,
        total_returns,
        available_day_count,
        rented_day_count,
        utilisation_percentage,
        loaded_at
    )
    SELECT 
        dii.inventory_key,
        aid.date_key,
        COALESCE(dir.total_rentals, 0),
        COALESCE(diret.total_returns, 0),
        -- Available day count (1 if active item, else 0)
        CASE WHEN dii.item_condition <> 'Damaged' AND dii.item_status <> 'Retired' THEN 1 ELSE 0 END AS available_day_count,
        COALESCE(ard.rented_day_count, 0) AS rented_day_count,
        -- Utilisation % = (rented_days / available_days) * 100
        CASE 
            WHEN dii.item_condition <> 'Damaged' AND dii.item_status <> 'Retired' THEN
                ROUND((COALESCE(ard.rented_day_count, 0)::NUMERIC / 1.0) * 100, 2)
            ELSE 0.00 
        END AS utilisation_percentage,
        CURRENT_TIMESTAMP
    FROM all_inv_days aid
    JOIN analytical.dim_inventory_item dii ON aid.inventory_id = dii.inventory_id
    JOIN analytical.dim_date dd ON aid.date_key = dd.date_key
    LEFT JOIN daily_item_rentals dir ON aid.inventory_id = dir.inventory_id AND aid.date_key = dir.date_key
    LEFT JOIN daily_item_returns diret ON aid.inventory_id = diret.inventory_id AND aid.date_key = diret.date_key
    LEFT JOIN active_rented_days ard ON aid.inventory_id = ard.inventory_id AND aid.date_key = ard.date_key
    ON CONFLICT (inventory_key, date_key) DO UPDATE SET
        total_rentals = EXCLUDED.total_rentals,
        total_returns = EXCLUDED.total_returns,
        available_day_count = EXCLUDED.available_day_count,
        rented_day_count = EXCLUDED.rented_day_count,
        utilisation_percentage = EXCLUDED.utilisation_percentage,
        loaded_at = CURRENT_TIMESTAMP;

    RAISE NOTICE 'ETL Execution Completed Successfully!';
END;
$$;



-- ----------------------------------------------------------------------------
-- 5. Helper Function for NiFi ExecuteSQL (returns a valid ResultSet)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION analytical.fn_run_full_etl()
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    CALL analytical.sp_run_full_etl();
    RETURN 'ETL Completed Successfully';
END;
$$;
