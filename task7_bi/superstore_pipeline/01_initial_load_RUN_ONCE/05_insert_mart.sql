-- ==========================================
-- 1. Products
-- ==========================================
INSERT INTO
	mart.dim_products (product_id, category, sub_category, product_name)
SELECT
	p.product_id,
	c.category::VARCHAR,
	c.sub_category,
	p.product_name
FROM
	core.products p
	JOIN core.categories c USING (category_id);

-- ==========================================
-- 2. Customers
-- ==========================================
INSERT INTO
	mart.dim_customers (
		customer_id,
		customer_name,
		segment,
		country,
		city,
		state,
		postal_code,
		region
	)
SELECT DISTINCT
	ON (c.customer_id)
	c.customer_id,
	c.customer_name,
	c.segment::VARCHAR,
	l.country,
	l.city,
	l.state,
	l.postal_code,
	l.region::VARCHAR
FROM
	core.customers c
	JOIN core.orders o ON c.customer_id = o.customer_id
	JOIN core.locations l ON o.location_id = l.location_id
ORDER BY
	c.customer_id,
	o.order_date DESC;

-- ==========================================
-- 3. Orders
-- ==========================================
INSERT INTO mart.dim_orders (order_id, ship_mode)
SELECT
    order_id,
    ship_mode::VARCHAR
FROM
    core.orders;

-- ==========================================
-- 4. Fact Sales
-- ==========================================
INSERT INTO
	mart.fact_sales (
		row_id,
		customer_sk,
		product_sk,
		order_sk,
		order_date_sk,
		ship_date_sk,
		sales,
		quantity,
		discount,
		profit
	)
	-- PART 1: CTE for core Order and Order Details tables
WITH
	raw_fact_data AS (
		SELECT
			od.row_id,
			od.order_id,
			od.product_id,
			o.customer_id,
			o.order_date,
			o.ship_date,
			od.sales,
			od.quantity,
			od.discount,
			od.profit
		FROM
			core.order_details od
			JOIN core.orders o ON od.order_id = o.order_id
	)
-- PART 2: Swapping text for surrogate keys
SELECT
    r.row_id,
    c_dim.customer_sk,       		  -- Translated
    p_dim.product_sk,        		  -- Translated
    o_dim.order_sk,      			  -- Translated
    d_order.date_sk AS order_date_sk, -- Translated
    d_ship.date_sk AS ship_date_sk,   -- Translated
    r.sales,
    r.quantity,
    r.discount,
    r.profit
FROM raw_fact_data r

-- Swap Product ID for Product SK
JOIN mart.dim_products p_dim
    ON r.product_id = p_dim.product_id

-- Swap Order ID for Order SK
JOIN mart.dim_orders o_dim
    ON r.order_id = o_dim.order_id

-- Swap Customer ID for Customer SK
JOIN mart.dim_customers c_dim
    ON r.customer_id = c_dim.customer_id

-- Swap Order Date for Date SK
JOIN mart.dim_dates d_order
    ON r.order_date = d_order.full_date

-- Swap Ship Date for Date SK
JOIN mart.dim_dates d_ship
    ON r.ship_date = d_ship.full_date
		ORDER BY row_id;