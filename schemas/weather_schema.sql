-- Schema generated for weather.parquet
-- Generated on 2025-02-12 08:52:58
-- Number of columns: 10

-- Table Definition
CREATE TABLE IF NOT EXISTS "weather" (
    "timestamp" VARCHAR(19) NOT NULL,
    "site_id" VARCHAR(8) NOT NULL,
    "airtemperature" NUMERIC(4,1),
    "cloudcoverage" SMALLINT,
    "dewtemperature" NUMERIC(4,1),
    "precipdepth1hr" SMALLINT,
    "precipdepth6hr" SMALLINT,
    "sealvlpressure" NUMERIC(5,1),
    "winddirection" SMALLINT,
    "windspeed" NUMERIC(3,1)
);

-- Column Information:
-- timestamp:
--   Type: object -> VARCHAR(19)
--   Nullable: False
--   Unique Values: 17544
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- site_id:
--   Type: object -> VARCHAR(8)
--   Nullable: False
--   Unique Values: 19
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- airtemperature:
--   Type: float64 -> NUMERIC(4,1)
--   Nullable: True
--   Unique Values: 646
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- cloudcoverage:
--   Type: float64 -> SMALLINT
--   Nullable: True
--   Unique Values: 10
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- dewtemperature:
--   Type: float64 -> NUMERIC(4,1)
--   Nullable: True
--   Unique Values: 553
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- precipdepth1hr:
--   Type: float64 -> SMALLINT
--   Nullable: True
--   Unique Values: 167
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- precipdepth6hr:
--   Type: float64 -> SMALLINT
--   Nullable: True
--   Unique Values: 168
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- sealvlpressure:
--   Type: float64 -> NUMERIC(5,1)
--   Nullable: True
--   Unique Values: 756
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- winddirection:
--   Type: float64 -> SMALLINT
--   Nullable: True
--   Unique Values: 59
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- windspeed:
--   Type: float64 -> NUMERIC(3,1)
--   Nullable: True
--   Unique Values: 73
--   Recommendations:
--     * Low cardinality - consider using as categorical
--