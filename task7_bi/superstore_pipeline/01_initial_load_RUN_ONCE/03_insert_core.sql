-- ==========================================
-- 1. Customers
-- ==========================================
INSERT INTO
	core.customers (customer_id, customer_name, segment)
SELECT DISTINCT
	customer_id,
	COALESCE(customer_name, 'UNKNOWN'),
	segment::core.segment_enum
FROM
	stg.initial_load;

-- ==========================================
-- 2. Locations
-- ==========================================
INSERT INTO
	core.locations (postal_code, country, region, state, city)
SELECT DISTINCT
	COALESCE(postal_code, 'UNKNOWN'),
	country,
	region::core.region_enum,
	state,
	city
FROM
	stg.initial_load;

-- ==========================================
-- 3. Categories
-- ==========================================
INSERT INTO
	core.categories (category, sub_category)
SELECT DISTINCT
	category::core.category_enum,
	COALESCE(sub_category, 'UNKNOWN')
FROM
	stg.initial_load;

-- ==========================================
-- 4. Products
-- ==========================================
INSERT INTO
	core.products (product_id, product_name, category_id)
SELECT DISTINCT
	ON (s.product_id) s.product_id,
	COALESCE(s.product_name, 'UNKNOWN'),
	c.category_id -- integer ID from Categories
FROM
	stg.initial_load s
	JOIN core.categories c ON s.sub_category = c.sub_category
ORDER BY
	s.product_id;

-- ==========================================
-- 5. Orders
-- ==========================================
INSERT INTO core.orders (
    order_id, customer_id, location_id, order_date, ship_date, ship_mode
)
SELECT DISTINCT ON (s.order_id)
    s.order_id,
    s.customer_id,
    l.location_id,
    s.order_date,
    s.ship_date,
    s.ship_mode::core.ship_mode_enum
FROM stg.initial_load s
JOIN core.locations l ON s.postal_code = l.postal_code
    AND s.country = l.country
    AND s.state = l.state
    AND s.city = l.city
ORDER BY s.order_id;

-- ==========================================
-- 6. Order Details
-- ==========================================
INSERT INTO core.order_details(row_id, order_id, product_id, sales, quantity, discount, profit)
SELECT
  row_id,
	order_id,
	product_id,
	sales,
	quantity,
	discount,
	profit
FROM stg.initial_load;