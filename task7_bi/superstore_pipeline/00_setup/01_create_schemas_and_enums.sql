-- ==========================================
-- Schemas
-- ==========================================
CREATE SCHEMA IF NOT EXISTS stg;

CREATE SCHEMA IF NOT EXISTS core;

CREATE SCHEMA IF NOT EXISTS mart;

-- ==========================================
-- ENUMs
-- ==========================================
DROP TYPE IF EXISTS core.ship_mode_enum CASCADE;
CREATE TYPE core.ship_mode_enum AS ENUM(
	'First Class',
	'Second Class',
	'Standard Class',
	'Same Day'
);

DROP TYPE IF EXISTS core.segment_enum CASCADE;
CREATE TYPE core.segment_enum AS ENUM('Consumer', 'Corporate', 'Home Office');

DROP TYPE IF EXISTS core.region_enum CASCADE;
CREATE TYPE core.region_enum AS ENUM(
	'Central',
	'East',
	'South',
	'West',
	'International'
);

DROP TYPE IF EXISTS core.category_enum CASCADE;
CREATE TYPE core.category_enum AS ENUM('Furniture', 'Office Supplies', 'Technology');
