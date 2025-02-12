-- Schema generated for metadata.parquet
-- Generated on 2025-02-12 08:50:14
-- Number of columns: 32

-- Table Definition
CREATE TABLE IF NOT EXISTS "metadata" (
    "building_id" VARCHAR(29) NOT NULL,
    "site_id" VARCHAR(8) NOT NULL,
    "building_id_kaggle" SMALLINT,
    "site_id_kaggle" SMALLINT,
    "primaryspaceusage" VARCHAR(29),
    "sub_primaryspaceusage" VARCHAR(49),
    "sqm" NUMERIC(6,1) NOT NULL,
    "sqft" INTEGER NOT NULL,
    "lat" NUMERIC(17,15),
    "lng" NUMERIC(18,16),
    "timezone" VARCHAR(13) NOT NULL,
    "electricity" VARCHAR(4),
    "hotwater" VARCHAR(4),
    "chilledwater" VARCHAR(4),
    "steam" VARCHAR(4),
    "water" VARCHAR(4),
    "irrigation" VARCHAR(4),
    "solar" VARCHAR(4),
    "gas" VARCHAR(4),
    "industry" VARCHAR(11),
    "subindustry" VARCHAR(37),
    "heatingtype" VARCHAR(26),
    "yearbuilt" SMALLINT,
    "date_opened" VARCHAR(9),
    "numberoffloors" SMALLINT,
    "occupants" SMALLINT,
    "energystarscore" VARCHAR(4),
    "eui" VARCHAR(6),
    "site_eui" VARCHAR(6),
    "source_eui" VARCHAR(6),
    "leed_level" VARCHAR(6),
    "rating" VARCHAR(4)
);

-- Column Information:
-- building_id:
--   Type: object -> VARCHAR(29)
--   Nullable: False
--   Unique Values: 1636
--   Recommendations:
--     * Consider as primary key candidate
--
-- site_id:
--   Type: object -> VARCHAR(8)
--   Nullable: False
--   Unique Values: 19
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- building_id_kaggle:
--   Type: float64 -> SMALLINT
--   Nullable: True
--   Unique Values: 1449
--   Recommendations:
--
-- site_id_kaggle:
--   Type: float64 -> SMALLINT
--   Nullable: True
--   Unique Values: 16
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- primaryspaceusage:
--   Type: object -> VARCHAR(29)
--   Nullable: True
--   Unique Values: 16
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- sub_primaryspaceusage:
--   Type: object -> VARCHAR(49)
--   Nullable: True
--   Unique Values: 104
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- sqm:
--   Type: float64 -> NUMERIC(6,1)
--   Nullable: False
--   Unique Values: 1571
--   Recommendations:
--
-- sqft:
--   Type: float64 -> INTEGER
--   Nullable: False
--   Unique Values: 1571
--   Recommendations:
--
-- lat:
--   Type: float64 -> NUMERIC(17,15)
--   Nullable: True
--   Unique Values: 15
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- lng:
--   Type: float64 -> NUMERIC(18,16)
--   Nullable: True
--   Unique Values: 15
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- timezone:
--   Type: object -> VARCHAR(13)
--   Nullable: False
--   Unique Values: 6
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- electricity:
--   Type: object -> VARCHAR(4)
--   Nullable: True
--   Unique Values: 1
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- hotwater:
--   Type: object -> VARCHAR(4)
--   Nullable: True
--   Unique Values: 1
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- chilledwater:
--   Type: object -> VARCHAR(4)
--   Nullable: True
--   Unique Values: 1
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- steam:
--   Type: object -> VARCHAR(4)
--   Nullable: True
--   Unique Values: 1
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- water:
--   Type: object -> VARCHAR(4)
--   Nullable: True
--   Unique Values: 1
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- irrigation:
--   Type: object -> VARCHAR(4)
--   Nullable: True
--   Unique Values: 1
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- solar:
--   Type: object -> VARCHAR(4)
--   Nullable: True
--   Unique Values: 1
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- gas:
--   Type: object -> VARCHAR(4)
--   Nullable: True
--   Unique Values: 1
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- industry:
--   Type: object -> VARCHAR(11)
--   Nullable: True
--   Unique Values: 4
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- subindustry:
--   Type: object -> VARCHAR(37)
--   Nullable: True
--   Unique Values: 12
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- heatingtype:
--   Type: object -> VARCHAR(26)
--   Nullable: True
--   Unique Values: 12
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- yearbuilt:
--   Type: float64 -> SMALLINT
--   Nullable: True
--   Unique Values: 117
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- date_opened:
--   Type: object -> VARCHAR(9)
--   Nullable: True
--   Unique Values: 21
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- numberoffloors:
--   Type: float64 -> SMALLINT
--   Nullable: True
--   Unique Values: 18
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- occupants:
--   Type: float64 -> SMALLINT
--   Nullable: True
--   Unique Values: 112
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- energystarscore:
--   Type: object -> VARCHAR(4)
--   Nullable: True
--   Unique Values: 33
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- eui:
--   Type: object -> VARCHAR(6)
--   Nullable: True
--   Unique Values: 263
--   Recommendations:
--
-- site_eui:
--   Type: object -> VARCHAR(6)
--   Nullable: True
--   Unique Values: 157
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- source_eui:
--   Type: object -> VARCHAR(6)
--   Nullable: True
--   Unique Values: 156
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- leed_level:
--   Type: object -> VARCHAR(6)
--   Nullable: True
--   Unique Values: 2
--   Recommendations:
--     * Low cardinality - consider using as categorical
--
-- rating:
--   Type: object -> VARCHAR(4)
--   Nullable: True
--   Unique Values: 9
--   Recommendations:
--     * Low cardinality - consider using as categorical
--