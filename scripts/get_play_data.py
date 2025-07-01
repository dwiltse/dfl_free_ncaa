#!/usr/bin/env python3
"""
College Football Data API Collector
Collect FBS play-by-play data and save as parquet files

Usage:
    python get_play_data.py --year 2023

Prerequisites:
    pip install requests pandas pyarrow python-dotenv

API Key Setup:
    Create .env file with: CFBD_API_KEY=your_api_key_here
    Or set environment variable: export CFBD_API_KEY=your_api_key_here
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def get_api_key():
    """
    Get API key from environment variable or prompt user
    """
    api_key = os.getenv('CFBD_API_KEY')
    
    if not api_key:
        logger.warning("CFBD_API_KEY not found in environment variables")
        api_key = input("Enter your CFBD API key: ").strip()
        
        if not api_key:
            raise ValueError("API key is required")
    
    return api_key


def get_headers(api_key):
    """
    Return headers for requests with Bearer token
    """
    return {"Authorization": f"Bearer {api_key}"}

def optimize_datatypes(df):
    """
    Optimize data types for efficient parquet storage
    """
    # Integer columns
    int_cols = ['offense_score', 'defense_score', 'period', 'yard_line', 
               'down', 'distance', 'yards_gained', 'week', 'year']
    
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int16')
    
    # String columns - use pandas string type for better performance
    string_cols = ['offense', 'defense', 'offense_conference', 'defense_conference',
                  'home', 'away', 'play_type', 'season_type']
    
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype('string')
    
    # Float columns
    if 'ppa' in df.columns:
        df['ppa'] = pd.to_numeric(df['ppa'], errors='coerce').astype('float32')
    
    return df

def get_plays_for_week(api_key, year, week, season_type='regular'):
    """
    Get play-by-play data for a specific week using requests
    """
    logger.info(f"Fetching {season_type} season, week {week}")
    url = "https://apinext.collegefootballdata.com/plays"
    params = {
        "year": year,
        "week": week,
        "seasonType": season_type,
        "classification": "fbs"
    }
    try:
        response = requests.get(url, headers=get_headers(api_key), params=params)
        response.raise_for_status()
        plays = response.json()
        if not plays:
            logger.warning(f"No plays returned for {season_type} week {week}")
            return pd.DataFrame()
        # Convert to DataFrame
        df = pd.DataFrame(plays)
        df = optimize_datatypes(df)
        logger.info(f"Retrieved {len(df)} plays")
        return df
    except Exception as e:
        logger.error(f"Error fetching {season_type} week {week}: {str(e)}")
        return pd.DataFrame()

def save_weekly_data(df, year, week, season_type, output_dir="."):
    """
    Save weekly data to parquet file
    """
    if df.empty:
        logger.warning(f"No data to save for {season_type} week {week}")
        return None
    
    filename = os.path.join(output_dir, f"{year}_week_{week}_{season_type}.parquet")
    
    try:
        df.to_parquet(filename, compression='snappy', index=False)
        file_size = os.path.getsize(filename) / 1024  # KB
        logger.info(f"Saved {filename} ({file_size:.1f} KB)")
        return filename
    except Exception as e:
        logger.error(f"Error saving {filename}: {str(e)}")
        return None

def collect_season_data(year, output_dir=".", delay=0.1):
    """
    Collect all play-by-play data for a season using requests
    """
    api_key = get_api_key()
    saved_files = []
    api_calls = 0
    # Regular season (Weeks 0-14)
    logger.info("Starting regular season collection...")
    regular_weeks = [0] + list(range(1, 15))
    for week in regular_weeks:
        df = get_plays_for_week(api_key, year, week, 'regular')
        api_calls += 1
        if not df.empty:
            filename = save_weekly_data(df, year, week, 'regular', output_dir)
            if filename:
                saved_files.append(filename)
        time.sleep(delay)  # Rate limiting
    # Postseason (Weeks 1-5)
    logger.info("Starting postseason collection...")
    postseason_weeks = list(range(1, 6))
    for week in postseason_weeks:
        df = get_plays_for_week(api_key, year, week, 'postseason')
        api_calls += 1
        if not df.empty:
            filename = save_weekly_data(df, year, week, 'postseason', output_dir)
            if filename:
                saved_files.append(filename)
        time.sleep(delay)  # Rate limiting
    return saved_files, api_calls

def main():
    """
    Main function to run the data collection
    """
    import argparse
    print("🏈 College Football Data Collector")
    print("=" * 40)

    parser = argparse.ArgumentParser(description="Collect NCAA FBS play-by-play data and save as parquet files.")
    parser.add_argument("--year", type=int, required=True, help="Season year (e.g. 2023, 2024, 2025)")
    parser.add_argument("--output_dir", type=str, default="data", help="Output directory for parquet files")
    parser.add_argument("--delay", type=float, default=0.1, help="Delay between API calls (seconds)")
    args = parser.parse_args()

    year = args.year
    output_dir = args.output_dir
    delay = args.delay

    os.makedirs(output_dir, exist_ok=True)
    start_time = datetime.now()

    try:
        saved_files, api_calls = collect_season_data(year, output_dir, delay)
        end_time = datetime.now()
        duration = end_time - start_time

        # Summary
        print(f"\n✅ Collection Complete!")
        print(f"📊 API calls made: {api_calls}")
        print(f"📁 Files saved: {len(saved_files)}")
        print(f"⏱️  Duration: {duration}")
        print(f"📂 Output directory: {output_dir}")

        # Calculate total size
        total_size = sum(os.path.getsize(f) for f in saved_files) / (1024 * 1024)
        print(f"💾 Total size: {total_size:.1f} MB")

        # List files
        print(f"\nCreated files:")
        for file in sorted(saved_files):
            size_kb = os.path.getsize(file) / 1024
            print(f"  {os.path.basename(file)} ({size_kb:.1f} KB)")

    except Exception as e:
        logger.error(f"Collection failed: {str(e)}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())
