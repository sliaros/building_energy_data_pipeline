-- Schema generated for raw.parquet
-- Generated on 2025-02-12 08:52:55
-- Number of columns: 4

-- Table Definition
CREATE TABLE IF NOT EXISTS "raw" (
    "timestamp" VARCHAR(19) NOT NULL,
    "building_id" VARCHAR(29) NOT NULL,
    "meter_reading" NUMERIC(17,16),
    "meter" VARCHAR(12) NOT NULL
);

-- Column Information:
-- timestamp:
--   Type: object -> VARCHAR(19)
--   Nullable: False
--   Unique Values: 17544
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- building_id:
--   Type: object -> VARCHAR(29)
--   Nullable: False
--   Unique Values: 1636
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- meter_reading:
--   Type: float64 -> NUMERIC(17,16)
--   Nullable: True
--   Unique Values: 5475953
--   Recommendations:
--
-- meter:
--   Type: object -> VARCHAR(12)
--   Nullable: False
--   Unique Values: 8
--   Recommendations:
--     * Consider adding an index
--     * Low cardinality - consider using as categorical
--