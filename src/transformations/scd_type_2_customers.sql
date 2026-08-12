-- Staging table for the raw ingestion load
CREATE OR REPLACE TABLE stg_customers (
    customer_id INT,
    first_name VARCHAR,
    last_name VARCHAR,
    city VARCHAR,
    tier VARCHAR,
    updated_at TIMESTAMP
);

-- SCD Type 2 dimension table
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key INT AUTOINCREMENT,
    customer_id INT,
    first_name VARCHAR,
    last_name VARCHAR,
    city VARCHAR,
    tier VARCHAR,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN
);

-- SCD Type 2 merge: close out the current record when tracked attributes change
MERGE INTO dim_customers AS target
USING stg_customers AS source
ON target.customer_id = source.customer_id
AND target.is_current = TRUE

WHEN MATCHED AND (
    target.city <> source.city OR
    target.tier <> source.tier
) THEN UPDATE SET
    target.effective_end_date = CURRENT_TIMESTAMP(),
    target.is_current = FALSE;

-- Insert new or changed records as the current version
INSERT INTO dim_customers (
    customer_id,
    first_name,
    last_name,
    city,
    tier,
    effective_start_date,
    effective_end_date,
    is_current
)
SELECT
    source.customer_id,
    source.first_name,
    source.last_name,
    source.city,
    source.tier,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
FROM stg_customers AS source
LEFT JOIN dim_customers AS target
    ON source.customer_id = target.customer_id
    AND target.is_current = TRUE
WHERE target.customer_id IS NULL
   OR target.city <> source.city
   OR target.tier <> source.tier;