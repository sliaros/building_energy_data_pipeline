-- Schema generated for metadata.parquet
-- Generated on 2025-02-11 10:42:06
-- Number of columns: 32

-- Table Definition
CREATE TABLE IF NOT EXISTS "metadata" (
    "building_id" VARCHAR(29),
    "site_id" VARCHAR(8),
    "building_id_kaggle" DOUBLE PRECISION,
    "site_id_kaggle" DOUBLE PRECISION,
    "primaryspaceusage" VARCHAR(29),
    "sub_primaryspaceusage" VARCHAR(49),
    "sqm" DOUBLE PRECISION,
    "sqft" DOUBLE PRECISION,
    "lat" DOUBLE PRECISION,
    "lng" DOUBLE PRECISION,
    "timezone" VARCHAR(13),
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
    "yearbuilt" DOUBLE PRECISION,
    "date_opened" VARCHAR(9),
    "numberoffloors" DOUBLE PRECISION,
    "occupants" DOUBLE PRECISION,
    "energystarscore" VARCHAR(4),
    "eui" VARCHAR(6),
    "site_eui" VARCHAR(6),
    "source_eui" VARCHAR(6),
    "leed_level" VARCHAR(6),
    "rating" VARCHAR(4)
);

-- Notes:
-- 1. Review and adjust data types and constraints as needed
-- 2. Consider adding appropriate primary key constraint
-- 3. Consider adding foreign key constraints if applicable
-- 4. Add indexes based on your query patterns

-- Original Type Mappings:
-- building_id: string -> VARCHAR(29)
-- site_id: string -> VARCHAR(8)
-- building_id_kaggle: double -> DOUBLE PRECISION
-- site_id_kaggle: double -> DOUBLE PRECISION
-- primaryspaceusage: string -> VARCHAR(29)
-- sub_primaryspaceusage: string -> VARCHAR(49)
-- sqm: double -> DOUBLE PRECISION
-- sqft: double -> DOUBLE PRECISION
-- lat: double -> DOUBLE PRECISION
-- lng: double -> DOUBLE PRECISION
-- timezone: string -> VARCHAR(13)
-- electricity: string -> VARCHAR(4)
-- hotwater: string -> VARCHAR(4)
-- chilledwater: string -> VARCHAR(4)
-- steam: string -> VARCHAR(4)
-- water: string -> VARCHAR(4)
-- irrigation: string -> VARCHAR(4)
-- solar: string -> VARCHAR(4)
-- gas: string -> VARCHAR(4)
-- industry: string -> VARCHAR(11)
-- subindustry: string -> VARCHAR(37)
-- heatingtype: string -> VARCHAR(26)
-- yearbuilt: double -> DOUBLE PRECISION
-- date_opened: string -> VARCHAR(9)
-- numberoffloors: double -> DOUBLE PRECISION
-- occupants: double -> DOUBLE PRECISION
-- energystarscore: string -> VARCHAR(4)
-- eui: string -> VARCHAR(6)
-- site_eui: string -> VARCHAR(6)
-- source_eui: string -> VARCHAR(6)
-- leed_level: string -> VARCHAR(6)
-- rating: string -> VARCHAR(4)