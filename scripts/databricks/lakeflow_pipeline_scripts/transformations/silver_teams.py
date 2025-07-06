# Databricks DLT Pipeline - Silver Layer for Teams (from teams_raw)
# File: silver_teams_dlt_pipeline.py

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Pipeline Parameters
catalog = spark.conf.get("catalog", "dfl_dev")

# =============================================================================
# SILVER LAYER - Cleaned and flattened teams data (Streaming Table for APPLY CHANGES INTO)
# =============================================================================

@dlt.table(
    name=f"{catalog}.ncaa_silver.teams_cleaned",
    comment="Silver layer - Cleaned and flattened teams data prepared for CDC gold layer",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_fail("valid_team_id", "team_id IS NOT NULL")
@dlt.expect_or_drop("valid_school_name", "school_name IS NOT NULL AND length(trim(school_name)) > 0")
@dlt.expect_or_drop("valid_abbreviation", "team_abbreviation IS NOT NULL AND length(trim(team_abbreviation)) > 0")
@dlt.expect("has_conference", "conference_name IS NOT NULL")
@dlt.expect("reasonable_coordinates", """
    venue_latitude IS NULL OR venue_longitude IS NULL OR 
    (venue_latitude BETWEEN -90 AND 90 AND venue_longitude BETWEEN -180 AND 180)
""")
@dlt.expect("reasonable_capacity", """
    venue_capacity IS NULL OR 
    (venue_capacity >= 1000 AND venue_capacity <= 200000)
""")
def silver_teams_cleaned():
    """
    Transforms bronze teams_raw data into clean, flattened format for CDC processing.
    Removes technical metadata and standardizes business data.
    Streaming table required for APPLY CHANGES INTO in gold layer.
    """
    
    return (
        dlt.read_stream(f"{catalog}.ncaa_bronze.teams_raw")
        .select(
            # Core team identifiers
            F.col("id").cast("bigint").alias("team_id"),
            
            # Team information - cleaned and standardized
            F.when(F.col("school").isNotNull(), 
                   F.trim(F.regexp_replace(F.col("school"), r"\s+", " ")))
             .alias("school_name"),
            
            F.when(F.col("abbreviation").isNotNull(),
                   F.upper(F.trim(F.regexp_replace(F.col("abbreviation"), r"[^A-Za-z0-9]", ""))))
             .alias("team_abbreviation"),
            
            F.when(F.col("mascot").isNotNull(),
                   F.initcap(F.trim(F.col("mascot"))))
             .alias("team_mascot"),
            
            # Conference and division - keep as-is per requirement
            F.when(F.col("conference").isNotNull(),
                   F.trim(F.col("conference")))
             .alias("conference_name"),
            
            F.when(F.col("division").isNotNull(),
                   F.trim(F.col("division")))
             .alias("division_name"),
            
            # Location fields - flattened from struct
            F.col("location.id").cast("bigint").alias("venue_id"),
            
            F.when(F.col("location.name").isNotNull(),
                   F.trim(F.col("location.name")))
             .alias("venue_name"),
            
            F.when(F.col("location.city").isNotNull(),
                   F.trim(F.col("location.city")))
             .alias("venue_city"),
            
            F.when(F.col("location.state").isNotNull(),
                   F.upper(F.trim(F.col("location.state"))))
             .alias("venue_state"),
            
            F.when(F.col("location.zip").isNotNull(),
                   F.trim(F.col("location.zip")))
             .alias("venue_zip"),
            
            F.col("location.capacity").cast("bigint").alias("venue_capacity"),
            F.col("location.constructionYear").cast("integer").alias("venue_construction_year"),
            
            # Round coordinates to 6 decimal places to prevent GPS drift false changes
            F.when(F.col("location.latitude").isNotNull(),
                   F.round(F.col("location.latitude").cast("double"), 6))
             .alias("venue_latitude"),
            
            F.when(F.col("location.longitude").isNotNull(),
                   F.round(F.col("location.longitude").cast("double"), 6))
             .alias("venue_longitude"),
            
            F.col("location.dome").cast("boolean").alias("is_dome"),
            F.col("location.grass").cast("boolean").alias("has_grass_field"),
            
            F.when(F.col("location.elevation").isNotNull(),
                   F.trim(F.col("location.elevation")))
             .alias("venue_elevation"),
            
            F.when(F.col("location.timezone").isNotNull(),
                   F.trim(F.col("location.timezone")))
             .alias("venue_timezone"),
            
            F.when(F.col("location.countryCode").isNotNull(),
                   F.upper(F.trim(F.col("location.countryCode"))))
             .alias("country_code"),
            
            # Extract first logo from logos array
            F.when(F.size(F.col("logos")) > 0,
                   F.element_at(F.col("logos"), 1))
             .alias("primary_logo_url"),
            
            # Clean twitter handle - preserve/add @ sign
            F.when(F.col("twitter").isNotNull(),
                   F.concat(F.lit("@"), 
                           F.regexp_replace(
                               F.lower(F.trim(
                                   F.regexp_replace(F.col("twitter"), r"^https?://(www\.)?twitter\.com/", "")
                               )),
                               r"^@", ""
                           )))
             .alias("twitter_handle"),
            
            # Business data for sequencing CDC
            F.col("data_year"),
            
            # Add derived business fields
            F.when(F.col("location.constructionYear").isNotNull(),
                   F.year(F.current_date()) - F.col("location.constructionYear").cast("integer"))
             .alias("venue_age"),
            
            F.when(F.col("location.capacity").isNotNull(),
                   F.when(F.col("location.capacity") >= 80000, "Mega")
                    .when(F.col("location.capacity") >= 60000, "Large") 
                    .when(F.col("location.capacity") >= 30000, "Medium")
                    .otherwise("Small"))
             .alias("capacity_tier"),
            
            # Geographic region based on state
            F.when(F.col("location.state").isin(["TX", "OK", "KS", "IA", "MO", "AR", "LA"]), "South Central")
             .when(F.col("location.state").isin(["FL", "GA", "AL", "MS", "TN", "KY", "SC", "NC", "VA", "WV"]), "Southeast")
             .when(F.col("location.state").isin(["CA", "OR", "WA", "AZ", "UT", "NV", "CO", "ID", "MT", "WY"]), "West")
             .when(F.col("location.state").isin(["NY", "PA", "NJ", "CT", "MA", "RI", "VT", "NH", "ME", "MD", "DE", "DC"]), "Northeast")
             .when(F.col("location.state").isin(["OH", "MI", "IN", "IL", "WI", "MN", "ND", "SD", "NE"]), "Midwest")
             .otherwise("Other")
             .alias("geographic_region"),
            
            # Data quality score
            F.round(
                (F.when(F.col("school").isNotNull(), 1).otherwise(0) +
                 F.when(F.col("abbreviation").isNotNull(), 1).otherwise(0) +
                 F.when(F.col("conference").isNotNull(), 1).otherwise(0) +
                 F.when(F.col("location.name").isNotNull(), 1).otherwise(0) +
                 F.when(F.col("location.capacity").isNotNull(), 1).otherwise(0)) / 5.0 * 100, 1
            ).alias("data_completeness_score"),
            
            # Processing metadata
            F.current_timestamp().alias("silver_processed_at")
        )
        .filter(F.col("team_id").isNotNull())  # Remove records without team ID
        .dropDuplicates(["team_id", "data_year"])  # Remove duplicates within same year
    )

# =============================================================================
# CONFIGURATION NOTES
# =============================================================================

"""
Silver Layer Configuration Instructions:

1. Prerequisites:
   - Bronze teams table must exist: {catalog}.ncaa_bronze.teams_raw
   - Set catalog parameter in DLT pipeline: {"catalog": "dfl_dev"}

2. Output table:
   - {catalog}.ncaa_silver.teams_cleaned

3. Streaming Table Benefits:
   - Required for APPLY CHANGES INTO in gold layer
   - Enables incremental processing for future expansions
   - Handles schema evolution automatically
   - Still efficient for annual teams data updates

4. Data transformations:
   - Flattens location struct into individual columns
   - Extracts first logo from logos array
   - Cleans and standardizes text fields
   - Preserves @ sign in Twitter handles (adds if missing)
   - Rounds coordinates to prevent GPS drift changes
   - Adds derived business fields (venue_age, capacity_tier, geographic_region)
   - Removes technical metadata (keeps only data_year for CDC sequencing)

5. Quality expectations:
   - Fails on missing team_id (critical for CDC)
   - Drops records missing school_name or team_abbreviation
   - Validates coordinate ranges and venue capacity
   - Tracks conference and location completeness

6. Usage for CDC:
   APPLY CHANGES INTO gold.dim_teams
   FROM silver.teams_cleaned  
   KEYS (team_id)
   SEQUENCE BY data_year
   COLUMNS * EXCEPT (silver_processed_at)
   STORED AS SCD TYPE 2

   Note: This silver table reads from bronze teams_raw table
"""