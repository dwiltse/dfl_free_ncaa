import os
import requests
from dotenv import load_dotenv
import pandas as pd
import argparse

"""
Script to fetch NCAA FBS teams data for a given year using the cfbd API and save as CSV and Parquet.
This script makes a single API call per year.
"""

def main():
    """
    Main function to download NCAA FBS teams data for a specified year and save it as JSON.
    
    Parses command-line arguments to get the year and output directory. Loads the API key from
    environment variables. Fetches the teams data from the cfbd API for the specified year.
    Saves the data as a JSON file in the output directory.

    Raises:
        RuntimeError: If the CFBD_API_KEY is not found in the environment variables.

    Command-line Arguments:
        --year: The season year for which to download the data (e.g., 2024).
        --output_dir: The directory where the JSON file will be saved (default is "data").
    """

    parser = argparse.ArgumentParser(description="Download NCAA FBS teams data for a given year.")
    parser.add_argument("--year", type=int, required=True, help="Season year (e.g. 2024)")
    parser.add_argument("--output_dir", type=str, default="data", help="Output directory")
    args = parser.parse_args()

    load_dotenv()
    api_key = os.getenv("CFBD_API_KEY")
    if not api_key:
        raise RuntimeError("CFBD_API_KEY not found in environment variables or .env file.")
    headers = {"Authorization": f"Bearer {api_key}"}
    # Use only the classic endpoint for FBS teams (this one works)
    url = f"https://api.collegefootballdata.com/teams/fbs?year={args.year}"
    print(f"Fetching FBS teams for {args.year} from {url} ...")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        try:
            teams = response.json()
        except Exception as e:
            print("Error decoding JSON from API response:", e)
            print("Status code:", response.status_code)
            print("Response text:\n", response.text)
            print("Response headers:\n", response.headers)
            return
    except requests.HTTPError as e:
        print(f"HTTP error: {e}")
        print("Status code:", response.status_code)
        print("Response text:\n", response.text)
        print("Response headers:\n", response.headers)
        return
    if not teams:
        print("No teams data returned.")
        return

    # Save as JSON in data/teams
    teams_dir = os.path.join(args.output_dir, "teams")
    os.makedirs(teams_dir, exist_ok=True)
    json_path = os.path.join(teams_dir, f"{args.year}_ncaa_team_data.json")
    with open(json_path, "w", encoding="utf-8") as f:
        import json
        json.dump(teams, f, ensure_ascii=False, indent=2)
    print(f"Saved {json_path}")

if __name__ == "__main__":
    main()
