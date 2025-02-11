-- Schema generated for raw.parquet
-- Generated on 2025-02-09 18:01:02
-- Number of columns: 4

-- Table Definition
CREATE TABLE IF NOT EXISTS "raw" (
    "timestamp" VARCHAR(19),
    "building_id" VARCHAR(29),
    "meter_reading" DOUBLE PRECISION,
    "meter" VARCHAR(12)
);

-- Notes:
-- 1. Review and adjust data types and constraints as needed
-- 2. Consider adding appropriate primary key constraint
-- 3. Consider adding foreign key constraints if applicable
-- 4. Add indexes based on your query patterns

-- Original Type Mappings:
-- timestamp: string -> VARCHAR(19)
-- building_id: string -> VARCHAR(29)
-- meter_reading: double -> DOUBLE PRECISION
-- meter: string -> VARCHAR(12)