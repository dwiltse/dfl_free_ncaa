# Databricks DLT Pipeline for Teams Data - Bronze Layer (Raw)
# File: teams_raw_bronze_dlt_pipeline.py

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Pipeline Parameters - Configure catalog in your DLT pipeline settings
catalog = spark.conf.get("catalog", "dfl_dev")  # Default to 'dfl_dev' if not specified

# =============================================================================
# BRONZE LAYER - Raw teams data ingestion (teams_raw)
# =============================================================================

@dlt.table(
    name=f"{catalog}.ncaa_bronze.teams_raw",
    comment="Bronze layer - Raw teams data from JSON files with audit fields",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_fail("valid_team_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_school_name", "school IS NOT NULL AND length(trim(school)) > 0")
@dlt.expect_or_drop("valid_abbreviation", "abbreviation IS NOT NULL AND length(trim(abbreviation)) > 0")
@dlt.expect("valid_conference", "conference IS NOT NULL AND length(trim(conference)) > 0")
@dlt.expect("valid_year_range", "data_year >= 2000 AND data_year <= year(current_date()) + 1")
@dlt.expect("location_has_basic_info", """
    location IS NOT NULL 
    AND (location.name IS NOT NULL OR location.city IS NOT NULL)
""")
@dlt.expect("reasonable_capacity", """
    location.capacity IS NULL 
    OR (location.capacity >= 1000 AND location.capacity <= 200000)
""")
def ncaa_bronze_teams_raw():
    """
    Ingests raw teams JSON data from S3 with audit fields.
    
    Note: Configure the source path directly in the cloudFiles.load() parameter below
    """
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiline", "true")  # Handle multiline JSON as specified
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Handle schema changes
        .load("s3://ncaadata/teams/")
        
        # Add audit and tracking fields
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("ingestion_date", F.current_date())
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("data_year", 
                   F.regexp_extract(F.col("source_file"), r"(\d{4})_ncaa_team", 1).cast("integer"))
        .withColumn("pipeline_run_id", F.expr("uuid()"))  # Unique identifier for this pipeline run
    )

# =============================================================================
# CONFIGURATION NOTES
# =============================================================================

"""
Pipeline Configuration Instructions:

1. Set up your DLT pipeline with this parameter:
   - catalog: Target catalog name (defaults to dfl_dev if not specified)

2. Pipeline Settings Example:
   {
     "catalog": "dfl_dev"
   }
   (Note: If no catalog parameter is provided, it defaults to "dfl_dev")

3. Source path is set to: s3://ncaadata/teams/

4. Managed Tables:
   - Tables will be managed by Databricks in Unity Catalog
   - Schema evolution handled automatically by Databricks
   - No external schema checkpoint locations needed

5. The pipeline will create managed table:
   - {catalog}.ncaa_bronze.teams_raw

6. Data includes all original JSON fields plus:
   - ingestion_timestamp: When record was processed
   - ingestion_date: Date of processing
   - source_file: Source file path
   - data_year: Year extracted from filename (e.g., 2024 from "2024_ncaa_team_data.json")
   - pipeline_run_id: Unique identifier for each pipeline run

7. Data Quality Expectations:
   - expect_or_fail("valid_team_id"): Fails pipeline if team ID is missing (critical field)
   - expect_or_drop("valid_school_name"): Drops records with missing/empty school names
   - expect_or_drop("valid_abbreviation"): Drops records with missing/empty abbreviations  
   - expect("valid_conference"): Tracks records with missing conference (retains but metrics tracked)
   - expect("valid_year_range"): Validates year is reasonable (2000 to current year + 1)
   - expect("location_has_basic_info"): Ensures location has either name or city
   - expect("reasonable_capacity"): Validates venue capacity is within reasonable bounds (1K-200K)

8. Usage:
   - Query: SELECT * FROM {catalog}.ncaa_bronze.teams_raw WHERE data_year = 2024
   - Track changes: SELECT id, school, conference, data_year FROM {catalog}.ncaa_bronze.teams_raw ORDER BY data_year
   - Monitor data quality: Check the "Data quality" tab in DLT UI for expectation metrics
"""