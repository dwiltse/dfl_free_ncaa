#!/usr/bin/env python3
"""
Simple test to get Nebraska Week 1 data - should be 1 API call
"""

import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def get_team_week_data(season=2024, week=1, team="Nebraska"):
    """Get play stats for a specific team, week, and season."""
    api_key = os.getenv('CFBD_API_KEY')
    if not api_key:
        api_key = input("Enter your CFBD API key: ").strip()
        if not api_key:
            print("❌ API key is required.")
            return None
    headers = {"Authorization": f"Bearer {api_key}"}
    url = "https://apinext.collegefootballdata.com/plays/stats"
    # Try using 'year' and 'seasonType' as per latest API docs
    params = {
        "year": season,  # 'year' instead of 'season'
        "week": week,
        "team": team,
        "seasonType": "regular",  # or 'postseason' if needed
        "limit": 2000
    }
    print(f"🏈 Getting {team} Week {week} play stats for {season}...")
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        print(f"\n✅ Success!")
        print(f"📊 Records returned: {len(data)}")
        print(f"🔢 API calls used: 1")
        if data:
            # Post-download filter for team
            filtered = [item for item in data if item.get('team', '').lower() == team.lower()]
            print(f"\n🔎 Post-download filter: {len(filtered)} records for team '{team}'")
            teams = set(item.get('team', 'Unknown') for item in filtered)
            opponents = set(item.get('opponent', 'Unknown') for item in filtered)
            games = set(item.get('gameId', 'Unknown') for item in filtered)
            stat_types = set(item.get('statType', 'Unknown') for item in filtered)
            print(f"\n📋 Data Summary (filtered):")
            print(f"   Teams: {teams}")
            print(f"   Opponents: {opponents}")
            print(f"   Games: {len(games)} games")
            print(f"   Stat Types: {stat_types}")
            if len(filtered) < 500 and len(games) <= 2:
                print(f"\n✅ This looks correct for {team} Week {week}!")
                df = pd.DataFrame(filtered)
                filename = f"{team.lower()}_week{week}_test.parquet"
                df.to_parquet(filename, index=False)
                print(f"💾 Saved to {filename}")
                return df
            else:
                print(f"\n⚠️  This seems like too much data for {team} Week {week}")
        else:
            print(f"❌ No data returned")
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def get_conference_week_data(season=2024, week=1, conference="B1G"):
    """Get play stats for a specific conference, week, and season, handling pagination."""
    api_key = os.getenv('CFBD_API_KEY')
    if not api_key:
        api_key = input("Enter your CFBD API key: ").strip()
        if not api_key:
            print("❌ API key is required.")
            return None
    headers = {"Authorization": f"Bearer {api_key}"}
    url = "https://apinext.collegefootballdata.com/plays/stats"
    all_data = []
    page = 1
    total_records = 0
    while True:
        params = {
            "year": season,
            "week": week,
            "conference": conference,
            "seasonType": "regular",
            "limit": 2000,
            "page": page
        }
        # Use a plain ASCII football emoji to avoid UnicodeEncodeError on Windows
        print(f"[FOOTBALL] Getting {conference} Week {week} play stats for {season} (page {page})...")
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            print(f"   Records returned: {len(data)}")
            if not data:
                break
            all_data.extend(data)
            total_records += len(data)
            if len(data) < 2000:
                break
            page += 1
        except Exception as e:
            print(f"❌ Error: {e}")
            break
    print(f"\n✅ Finished! Total records for {conference} Week {week}: {total_records}")
    if all_data:
        df = pd.DataFrame(all_data)
        # Save to data/play_stats/ subfolder in dfl_free_ncaa (project root)
        output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "play_stats")
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{conference.lower()}_week{week}_all.parquet"
        filepath = os.path.join(output_dir, filename)
        df.to_parquet(filepath, index=False)
        print(f"💾 Saved to {filepath}")
        print(f"Teams in data: {set(df['team'])}")
        print(f"Games: {set(df['gameId'])}")
        print(f"Stat Types: {set(df['statType'])}")
        return df
    else:
        print("❌ No data returned")
        return None

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Test CFB play stats API for a single team/week or all teams in a conference.")
    parser.add_argument("--season", type=int, default=2024, help="Season year (e.g. 2024)")
    parser.add_argument("--week", type=int, default=1, help="Week number (e.g. 1)")
    parser.add_argument("--conference", type=str, default="B1G", help="Conference name (e.g. B1G)")
    args = parser.parse_args()

    print(f"🧪 Testing {args.conference} Week {args.week} Data Collection")
    print("=" * 50)

    # Run for all of Big Ten for week 1
    df = get_conference_week_data(season=args.season, week=args.week, conference=args.conference)

    print(f"\n🎯 If this works correctly:")
    print(f"   - You should see all records for {args.conference} in week {args.week}")
    print(f"   - Multiple API calls if >2000 records")
    print(f"   - Data saved to parquet file")