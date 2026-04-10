-- ==========================================
-- 1. Create the Initial Load (around 80% of data)
-- ==========================================
DROP TABLE IF EXISTS stg.initial_load;

CREATE TABLE stg.initial_load AS
SELECT
	*
FROM
	stg.superstore_dump
WHERE
	segment IN ('Consumer', 'Corporate');

-- ==========================================
-- 2. Create Secondary Load Base (The "New Records" - around 20% of data)
-- ==========================================
DROP TABLE IF EXISTS stg.secondary_load;

CREATE TABLE stg.secondary_load AS
SELECT
	*
FROM
	stg.superstore_dump
WHERE
	segment = 'Home Office';

-- ==========================================
-- 3. Inject exact duplicates into Secondary Load
-- Takes 5 exact rows from the initial load
-- ==========================================
INSERT INTO
	stg.secondary_load
SELECT
	*
FROM
	stg.initial_load
LIMIT
	5;

-- ==========================================
-- 4. Inject SCD Type 1 Changes (name correction)
-- Takes 5 existing rows, alters the customer_name, and puts them in the secondary load
-- ==========================================
INSERT INTO
	stg.secondary_load
SELECT
	row_id,
	order_id,
	order_date,
	ship_date,
	ship_mode,
	customer_id,
	'(Corrected) ' || customer_name, -- SCD 1 change
	segment,
	country,
	city,
	state,
	postal_code,
	region,
	product_id,
	category,
	sub_category,
	product_name,
	sales,
	quantity,
	discount,
	profit
FROM
	stg.initial_load
OFFSET
	10
LIMIT
	5;

-- ==========================================
-- 5. Inject SCD Type 2 Changes (region change)
-- Takes 5 different existing rows, alters the region, and puts them in the secondary load
-- ==========================================
INSERT INTO
	stg.secondary_load
SELECT
	row_id,
	order_id,
	order_date,
	ship_date,
	ship_mode,
	customer_id,
	customer_name,
	segment,
	country,
	city,
	state,
	postal_code,
	'International', -- SCD 2 change
	product_id,
	category,
	sub_category,
	product_name,
	sales,
	quantity,
	discount,
	profit
FROM
	stg.initial_load
OFFSET
	20
LIMIT
	5;