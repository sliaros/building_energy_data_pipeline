-- Schema generated for weather.parquet
-- Generated on 2025-02-11 10:43:25
-- Number of columns: 10

-- Table Definition
CREATE TABLE IF NOT EXISTS "weather" (
    "timestamp" VARCHAR(19),
    "site_id" VARCHAR(8),
    "airtemperature" DOUBLE PRECISION,
    "cloudcoverage" DOUBLE PRECISION,
    "dewtemperature" DOUBLE PRECISION,
    "precipdepth1hr" DOUBLE PRECISION,
    "precipdepth6hr" DOUBLE PRECISION,
    "sealvlpressure" DOUBLE PRECISION,
    "winddirection" DOUBLE PRECISION,
    "windspeed" DOUBLE PRECISION
);

-- Notes:
-- 1. Review and adjust data types and constraints as needed
-- 2. Consider adding appropriate primary key constraint
-- 3. Consider adding foreign key constraints if applicable
-- 4. Add indexes based on your query patterns

-- Original Type Mappings:
-- timestamp: string -> VARCHAR(19)
-- site_id: string -> VARCHAR(8)
-- airtemperature: double -> DOUBLE PRECISION
-- cloudcoverage: double -> DOUBLE PRECISION
-- dewtemperature: double -> DOUBLE PRECISION
-- precipdepth1hr: double -> DOUBLE PRECISION
-- precipdepth6hr: double -> DOUBLE PRECISION
-- sealvlpressure: double -> DOUBLE PRECISION
-- winddirection: double -> DOUBLE PRECISION
-- windspeed: double -> DOUBLE PRECISION