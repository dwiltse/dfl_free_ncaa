#!/usr/bin/env python3
"""
College Football Data API Extractor
Extract play stats data and save as parquet files for S3/Databricks upload

Usage:
    python get_ncaa_play_stats_data.py --season 2024 --weeks 1,2,3,4
    python get_ncaa_play_stats_data.py --season 2024 --conferences SEC --weeks 1-5

Prerequisites:
    pip install requests pandas pyarrow python-dotenv tqdm
"""

import requests
import pandas as pd
import time
import os
import argparse
from datetime import datetime
from dotenv import load_dotenv
import logging
from tqdm import tqdm

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Conference mappings (official API abbreviations)
CONFERENCES = {
    "big_ten": "B1G",
    "b1g": "B1G", 
    "sec": "SEC",
    "big_12": "B12",
    "b12": "B12",
    "acc": "ACC",
    "pac_12": "PAC",
    "pac": "PAC",
    "aac": "AAC",
    "mwc": "MW",
    "cusa": "CUSA",
    "mac": "MAC",
    "sun_belt": "SBC"
}

def get_api_key():
    """Get API key from environment or user input"""
    api_key = os.getenv('CFBD_API_KEY')
    if not api_key:
        logger.warning("CFBD_API_KEY not found in environment variables")
        api_key = input("Enter your CFBD API key: ").strip()
        if not api_key:
            raise ValueError("API key is required")
    return api_key

def get_headers(api_key):
    """Create headers for API requests"""
    return {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": "CFB-Data-Extractor/1.0"
    }

def optimize_datatypes(df):
    """Optimize pandas datatypes for better parquet compression"""
    
    # Integer columns
    int_cols = ['gameId', 'season', 'week', 'teamScore', 'opponentScore', 
                'period', 'yardsToGoal', 'down', 'distance', 'stat']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    # String columns  
    string_cols = ['team', 'conference', 'opponent', 'driveId', 'playId', 
                   'athleteId', 'athleteName', 'statType']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype('string')
    
    # Clock columns (from nested structure)
    clock_cols = ['clock_minutes', 'clock_seconds']
    for col in clock_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    return df

def flatten_clock_data(data):
    """Flatten nested clock structure"""
    for item in data:
        clock = item.pop("clock", {})
        item["clock_minutes"] = clock.get("minutes")
        item["clock_seconds"] = clock.get("seconds")
    return data

def get_play_stats_for_week(api_key, season, week, conferences=None):
    """Get play stats for a specific week and conferences"""
    
    logger.info(f"Fetching play stats for season {season}, week {week}")
    url = "https://apinext.collegefootballdata.com/plays/stats"
    
    all_stats = []
    page = 1
    api_calls = 0
    
    # Prepare conference parameter
    if conferences:
        conference_param = ','.join(conferences)
    else:
        conference_param = None
    
    while True:
        params = {
            "season": season,
            "week": week,
            "limit": 2000,
            "page": page
        }
        
        if conference_param:
            params["conference"] = conference_param
        
        try:
            response = requests.get(url, headers=get_headers(api_key), params=params)
            response.raise_for_status()
            api_calls += 1
            
            try:
                stats = response.json()
            except Exception as e:
                logger.error(f"Non-JSON response for week {week}, page {page}: {response.text}")
                break
            
            if not stats:
                logger.info(f"No more data for week {week}, page {page}")
                break
            
            all_stats.extend(stats)
            logger.info(f"Retrieved {len(stats)} records from page {page} (Total API calls: {api_calls})")
            
            # Break if we got less than limit (last page)
            if len(stats) < 2000:
                break
                
            page += 1
            
            # Small delay to be respectful to API
            time.sleep(0.1)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for week {week}, page {page}: {str(e)}")
            break
        except Exception as e:
            logger.error(f"Unexpected error for week {week}, page {page}: {str(e)}")
            break
    
    if not all_stats:
        logger.warning(f"No play stats returned for week {week}")
        return pd.DataFrame(), api_calls
    
    # Flatten clock data
    all_stats = flatten_clock_data(all_stats)
    
    # Create DataFrame
    df = pd.DataFrame(all_stats)
    df = optimize_datatypes(df)
    
    logger.info(f"Week {week}: Retrieved {len(df)} total records with {api_calls} API calls")
    return df, api_calls

def save_parquet_file(df, season, week, conferences, output_dir):
    """Save DataFrame as parquet file with organized naming"""
    
    if df.empty:
        logger.warning(f"No data to save for week {week}")
        return None
    
    # Create filename
    conf_str = "_".join(conferences) if conferences else "all_conferences"
    filename = f"{season}_play_stats_week_{week:02d}_{conf_str}.parquet"
    filepath = os.path.join(output_dir, filename)
    
    try:
        df.to_parquet(filepath, compression='snappy', index=False)
        file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
        logger.info(f"✅ Saved {filename} ({file_size:.2f} MB, {len(df)} records)")
        return filepath
    except Exception as e:
        logger.error(f"❌ Error saving {filename}: {str(e)}")
        return None

def parse_weeks(weeks_str):
    """Parse weeks string into list of integers"""
    weeks = []
    
    for part in weeks_str.split(','):
        part = part.strip()
        if '-' in part:
            # Range like "1-5"
            start, end = map(int, part.split('-'))
            weeks.extend(range(start, end + 1))
        else:
            # Single week like "3"
            weeks.append(int(part))
    
    return sorted(list(set(weeks)))  # Remove duplicates and sort

def parse_conferences(conf_str):
    """Parse conference string into API abbreviations"""
    if not conf_str:
        return None
    
    conferences = []
    for conf in conf_str.lower().split(','):
        conf = conf.strip()
        if conf in CONFERENCES:
            conferences.append(CONFERENCES[conf])
        else:
            # Assume it's already an API abbreviation
            conferences.append(conf.upper())
    
    return conferences

def main():
    print("🏈 College Football Play Stats Extractor")
    print("=" * 50)
    
    parser = argparse.ArgumentParser(description="Extract NCAA play stats data to parquet files")
    parser.add_argument("--season", type=int, required=True, 
                       help="Season year (e.g., 2023, 2024)")
    parser.add_argument("--weeks", type=str, required=True,
                       help="Weeks to collect (e.g., '1,2,3' or '1-5' or '1,3-5,8')")
    parser.add_argument("--conferences", type=str, default=None,
                       help="Conferences to collect (e.g., 'big_ten,sec' or 'B1G,SEC')")
    parser.add_argument("--output_dir", type=str, default="./cfb_data",
                       help="Output directory for parquet files")
    
    args = parser.parse_args()
    
    # Parse inputs
    try:
        weeks = parse_weeks(args.weeks)
        conferences = parse_conferences(args.conferences)
        
        print(f"📅 Season: {args.season}")
        print(f"📊 Weeks: {weeks}")
        print(f"🏟️  Conferences: {conferences if conferences else 'All conferences'}")
        print(f"📁 Output directory: {args.output_dir}")
        
    except ValueError as e:
        logger.error(f"Error parsing arguments: {e}")
        return 1
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Get API key
    try:
        api_key = get_api_key()
    except ValueError as e:
        logger.error(f"API key error: {e}")
        return 1
    
    # Start collection
    start_time = datetime.now()
    saved_files = []
    total_api_calls = 0
    total_records = 0
    
    print(f"\n🚀 Starting data collection...")
    
    # Collect data for each week
    for week in tqdm(weeks, desc="Collecting weeks"):
        try:
            df, api_calls = get_play_stats_for_week(api_key, args.season, week, conferences)
            total_api_calls += api_calls
            
            if not df.empty:
                filepath = save_parquet_file(df, args.season, week, conferences, args.output_dir)
                if filepath:
                    saved_files.append(filepath)
                    total_records += len(df)
            
            # Safety check for API limits
            if total_api_calls >= 50:  # Conservative limit
                logger.warning(f"⚠️  Approaching API limits ({total_api_calls} calls). Consider stopping.")
            
        except Exception as e:
            logger.error(f"Error processing week {week}: {e}")
            continue
    
    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n✅ Collection Complete!")
    print(f"📊 API calls made: {total_api_calls}")
    print(f"📁 Files saved: {len(saved_files)}")
    print(f"📈 Total records: {total_records:,}")
    print(f"⏱️  Duration: {duration}")
    
    if saved_files:
        total_size = sum(os.path.getsize(f) for f in saved_files) / (1024 * 1024)
        print(f"💾 Total size: {total_size:.1f} MB")
        print(f"\n📋 Created files:")
        for file in sorted(saved_files):
            size_mb = os.path.getsize(file) / (1024 * 1024)
            print(f"  📄 {os.path.basename(file)} ({size_mb:.2f} MB)")
        
        print(f"\n📤 Ready for S3 upload:")
        print(f"   aws s3 cp {args.output_dir}/ s3://your-bucket/player_stats/{args.season}/ --recursive")
    
    return 0

if __name__ == "__main__":
    exit(main())