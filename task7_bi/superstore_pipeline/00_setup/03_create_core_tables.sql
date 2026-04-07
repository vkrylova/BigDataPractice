-- ==========================================
-- 1. Customers
-- ==========================================
DROP TABLE IF EXISTS core.customers CASCADE;

CREATE TABLE core.customers (
	customer_id VARCHAR(50) PRIMARY KEY,
	customer_name VARCHAR(100) NOT NULL,
	segment core.segment_enum
);

-- ==========================================
-- 2. Locations
-- ==========================================
DROP TABLE IF EXISTS core.locations CASCADE;

CREATE TABLE core.locations (
	location_id SERIAL PRIMARY KEY,
	postal_code VARCHAR(20) NOT NULL,
	country VARCHAR(50),
	region core.region_enum,
	state VARCHAR(50),
	city VARCHAR(50)
);

-- ==========================================
-- 3. Categories
-- ==========================================
DROP TABLE IF EXISTS core.categories CASCADE;

CREATE TABLE core.categories (
	category_id SERIAL PRIMARY KEY,
	category core.category_enum,
	sub_category VARCHAR(50) NOT NULL
);


-- ==========================================
-- 4. Products
-- ==========================================
DROP TABLE IF EXISTS core.products CASCADE;

CREATE TABLE core.products (
	product_id VARCHAR(50) PRIMARY KEY,
	product_name VARCHAR(255) NOT NULL,
	category_id INTEGER NOT NULL REFERENCES core.categories (category_id)
);


-- ==========================================
-- 5. Orders
-- ==========================================
DROP TABLE IF EXISTS core.orders CASCADE;

CREATE TABLE core.orders (
	order_id VARCHAR(50) PRIMARY KEY,
	customer_id VARCHAR(50) NOT NULL REFERENCES core.customers (customer_id),
	location_id INTEGER NOT NULL REFERENCES core.locations (location_id),
	order_date DATE NOT NULL
		CHECK (order_date <= CURRENT_DATE)
		CHECK (order_date >= '2010-01-01'),
	ship_date DATE CHECK(ship_date >= order_date),
	ship_mode core.ship_mode_enum
);


-- ==========================================
-- 6. Order Details
-- ==========================================
DROP TABLE IF EXISTS core.order_details CASCADE;

CREATE TABLE core.order_details (
	row_id INTEGER PRIMARY KEY,
	order_id VARCHAR(50) NOT NULL REFERENCES core.orders (order_id),
	product_id VARCHAR(50) NOT NULL REFERENCES core.products (product_id),
	sales NUMERIC(10, 4) CHECK (sales >= 0),
	quantity INTEGER CHECK (quantity > 0),
	discount NUMERIC(4, 2) CHECK (discount >= 0 AND discount <= 1),
	profit NUMERIC(10, 4)
);