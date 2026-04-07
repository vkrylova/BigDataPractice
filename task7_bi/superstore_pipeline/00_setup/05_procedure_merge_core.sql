CREATE OR REPLACE PROCEDURE core.run_daily_merge()
LANGUAGE plpgsql
AS $$
BEGIN
    -- ==========================================
    -- 1. Customers
    -- ==========================================
    MERGE INTO core.customers target USING (
        SELECT DISTINCT
           customer_id,
           COALESCE(customer_name, 'UNKNOWN') AS customer_name,
           segment::core.segment_enum
        FROM stg.secondary_load
    ) source ON target.customer_id = source.customer_id
    WHEN MATCHED AND (
        target.customer_name <> source.customer_name
        OR target.segment <> source.segment
    ) THEN UPDATE SET
        customer_name = source.customer_name,
        segment = source.segment
    WHEN NOT MATCHED THEN INSERT (customer_id, customer_name, segment)
    VALUES (source.customer_id, source.customer_name, source.segment);

    -- ==========================================
    -- 2. Locations
    -- ==========================================
    MERGE INTO core.locations target USING (
        SELECT DISTINCT
           COALESCE(postal_code, 'UNKNOWN') AS postal_code,
           country,
           region::core.region_enum,
           state,
           city
        FROM stg.secondary_load
        WHERE postal_code IS NOT NULL
    ) source ON target.postal_code = source.postal_code
    WHEN MATCHED AND (
        target.country <> source.country
        OR target.region <> source.region
        OR target.state <> source.state
        OR target.city <> source.city
    ) THEN UPDATE SET
        country = source.country,
        region = source.region,
        state = source.state,
        city = source.city
    WHEN NOT MATCHED THEN INSERT (postal_code, country, region, state, city)
    VALUES (source.postal_code, source.country, source.region, source.state, source.city);

    -- ==========================================
    -- 3. Categories
    -- ==========================================
    MERGE INTO core.categories target USING (
        SELECT DISTINCT
           category::core.category_enum,
           COALESCE(sub_category, 'UNKNOWN') AS sub_category
        FROM stg.secondary_load
    ) source ON target.sub_category = source.sub_category
    WHEN MATCHED AND (
        target.category <> source.category
    ) THEN UPDATE SET
        category = source.category
    WHEN NOT MATCHED THEN INSERT (category, sub_category)
    VALUES (source.category, source.sub_category);

    -- ==========================================
    -- 4. Products
    -- ==========================================
    MERGE INTO core.products target USING (
        SELECT DISTINCT ON (s.product_id)
           s.product_id,
           COALESCE(s.product_name, 'UNKNOWN') AS product_name,
           c.category_id
        FROM stg.secondary_load s
        JOIN core.categories c ON s.sub_category = c.sub_category
        ORDER BY s.product_id
    ) source ON target.product_id = source.product_id
    WHEN MATCHED AND (
        target.product_name <> source.product_name
        OR target.category_id <> source.category_id
    ) THEN UPDATE SET
        product_name = source.product_name,
        category_id = source.category_id
    WHEN NOT MATCHED THEN INSERT (product_id, product_name, category_id)
    VALUES (source.product_id, source.product_name, source.category_id);

    -- ==========================================
    -- 5. Orders
    -- ==========================================
    MERGE INTO core.orders target USING (
        SELECT DISTINCT ON (s.order_id)
            s.order_id,
            s.customer_id,
            l.location_id,
            s.order_date,
            s.ship_date,
            s.ship_mode::core.ship_mode_enum
        FROM stg.secondary_load s
        JOIN core.locations l ON s.postal_code = l.postal_code
            AND s.country = l.country
            AND s.state = l.state
            AND s.city = l.city
        ORDER BY s.order_id
    ) source ON target.order_id = source.order_id
    WHEN MATCHED AND (
        target.ship_date <> source.ship_date
        OR target.ship_mode <> source.ship_mode
        OR target.location_id <> source.location_id
    ) THEN UPDATE SET
        ship_date = source.ship_date,
        ship_mode = source.ship_mode,
        location_id = source.location_id
    WHEN NOT MATCHED THEN INSERT (order_id, customer_id, location_id, order_date, ship_date, ship_mode)
    VALUES (source.order_id, source.customer_id, source.location_id, source.order_date, source.ship_date, source.ship_mode);

    -- ==========================================
    -- 6. Order Details
    -- ==========================================
    MERGE INTO core.order_details target USING (
        SELECT
            row_id,
            order_id,
            product_id,
            sales,
            quantity,
            discount,
            profit
        FROM stg.secondary_load
    ) source ON target.row_id = source.row_id
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
    WHEN NOT MATCHED THEN INSERT (row_id, order_id, product_id, sales, quantity, discount, profit)
    VALUES (source.row_id, source.order_id, source.product_id, source.sales, source.quantity, source.discount, source.profit);

    -- Print a success message to the console
    RAISE NOTICE 'Core layer successfully merged with daily data!';
END;
$$;