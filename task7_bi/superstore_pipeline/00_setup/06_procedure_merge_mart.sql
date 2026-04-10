CREATE OR REPLACE PROCEDURE mart.run_daily_merge()
LANGUAGE plpgsql
AS $$
BEGIN

    -- ==========================================
    -- 1. SCD Type 1 (Overwrite) for Products
    -- ==========================================
    MERGE INTO mart.dim_products target
    USING (
        SELECT DISTINCT ON (s.product_id)
            s.product_id,
            c.category::VARCHAR,
            c.sub_category,
            p.product_name
        FROM stg.secondary_load s
        JOIN core.products p ON s.product_id = p.product_id
        JOIN core.categories c ON p.category_id = c.category_id
        ORDER BY s.product_id
    ) source
    ON target.product_id = source.product_id
    WHEN MATCHED AND (
        target.product_name <> source.product_name OR
        target.category <> source.category
    ) THEN UPDATE SET
        product_name = source.product_name,
        category = source.category,
        sub_category = source.sub_category
    WHEN NOT MATCHED THEN INSERT (product_id, category, sub_category, product_name)
    VALUES (source.product_id, source.category, source.sub_category, source.product_name);

    -- ==========================================
    -- 2. SCD Type 2: History for Customers
    -- ==========================================

    -- STEP 1: Retire the old records if something changed
    UPDATE mart.dim_customers target
    SET
        is_current = FALSE,
        valid_to = CURRENT_DATE
    FROM (
        SELECT DISTINCT ON (s.customer_id)
            c.customer_id,
            c.customer_name,
            c.segment::VARCHAR,
            l.country,
            l.city,
            l.state,
            l.postal_code,
            l.region::VARCHAR
        FROM stg.secondary_load s
        JOIN core.customers c ON s.customer_id = c.customer_id
        JOIN core.orders o ON c.customer_id = o.customer_id
        JOIN core.locations l ON o.location_id = l.location_id
        ORDER BY s.customer_id, o.order_date DESC
    ) fresh_data
    WHERE target.customer_id = fresh_data.customer_id
      AND target.is_current = TRUE
      AND (
          target.region <> fresh_data.region OR
          target.postal_code <> fresh_data.postal_code OR
          target.customer_name <> fresh_data.customer_name OR
          target.segment <> fresh_data.segment
      );

    -- STEP 2: Insert brand new records
    INSERT INTO mart.dim_customers (
        customer_id, customer_name, segment, country, city, state, postal_code, region
    )
    SELECT
        fresh_data.customer_id,
        fresh_data.customer_name,
        fresh_data.segment,
        fresh_data.country,
        fresh_data.city,
        fresh_data.state,
        fresh_data.postal_code,
        fresh_data.region
    FROM (
        SELECT DISTINCT ON (s.customer_id)
            c.customer_id,
            c.customer_name,
            c.segment::VARCHAR,
            l.country,
            l.city,
            l.state,
            l.postal_code,
            l.region::VARCHAR
        FROM stg.secondary_load s
        JOIN core.customers c ON s.customer_id = c.customer_id
        JOIN core.orders o ON c.customer_id = o.customer_id
        JOIN core.locations l ON o.location_id = l.location_id
        ORDER BY s.customer_id, o.order_date DESC
    ) fresh_data
    WHERE NOT EXISTS (
        SELECT 1
        FROM mart.dim_customers active_records
        WHERE active_records.customer_id = fresh_data.customer_id
          AND active_records.is_current = TRUE
    );

    -- ==========================================
    -- 3. Orders
    -- ==========================================
    MERGE INTO mart.dim_orders AS target
    USING (
        SELECT
            order_id,
            ship_mode::VARCHAR AS ship_mode
        FROM core.orders
    ) AS source
    ON target.order_id = source.order_id
    WHEN MATCHED AND target.ship_mode IS DISTINCT FROM source.ship_mode THEN
        UPDATE SET ship_mode = source.ship_mode
    WHEN NOT MATCHED THEN
        INSERT (order_id, ship_mode)
        VALUES (source.order_id, source.ship_mode);

    -- ==========================================
    -- 4. Fact Sales
    -- ==========================================
    MERGE INTO mart.fact_sales target
    USING (
        WITH raw_daily_data AS (
            SELECT
                s.row_id,
                s.order_id,
                s.product_id,
                o.customer_id,
                o.order_date,
                o.ship_date,
                s.sales,
                s.quantity,
                s.discount,
                s.profit
            FROM stg.secondary_load s
            JOIN core.orders o ON s.order_id = o.order_id
        )
        SELECT
            r.row_id,
            c.customer_sk,
            p.product_sk,
            o_dim.order_sk,
            d_order.date_sk AS order_date_sk,
            d_ship.date_sk AS ship_date_sk,
            r.sales,
            r.quantity,
            r.discount,
            r.profit
        FROM raw_daily_data r
        JOIN mart.dim_products p ON r.product_id = p.product_id
        JOIN mart.dim_orders o_dim ON r.order_id = o_dim.order_id
        JOIN mart.dim_customers c ON r.customer_id = c.customer_id AND c.is_current = TRUE
        JOIN mart.dim_dates d_order ON r.order_date = d_order.full_date
        JOIN mart.dim_dates d_ship ON r.ship_date = d_ship.full_date
    ) source
    ON target.row_id = source.row_id
    WHEN MATCHED AND (
        target.sales <> source.sales OR
        target.quantity <> source.quantity OR
        target.discount <> source.discount OR
        target.profit <> source.profit
    ) THEN UPDATE SET
        sales = source.sales,
        quantity = source.quantity,
        discount = source.discount,
        profit = source.profit
    WHEN NOT MATCHED THEN INSERT (
        row_id, customer_sk, product_sk, order_sk, order_date_sk, ship_date_sk,
        sales, quantity, discount, profit
    ) VALUES (
        source.row_id, source.customer_sk, source.product_sk, source.order_sk,
        source.order_date_sk, source.ship_date_sk,
        source.sales, source.quantity, source.discount, source.profit
    );

    -- Print a success message to the console
    RAISE NOTICE 'Mart layer updated with today''s data!';
END;
$$;