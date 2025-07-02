import os
import requests
from dotenv import load_dotenv
import pandas as pd
import argparse

def main():
    import time
    """
    Main function to download NCAA player data for a specified year and save it as CSV.
    Parses command-line arguments to get the year and output directory. Loads the API key from
    environment variables. Fetches the player data from the cfbd API for the specified year.
    Saves the data as a CSV file in the output directory.
    """
    parser = argparse.ArgumentParser(description="Download NCAA player data for a given year.")
    parser.add_argument("--year", type=int, required=True, help="Season year (e.g. 2024)")
    parser.add_argument("--output_dir", type=str, default="data", help="Output directory")
    args = parser.parse_args()

    load_dotenv()
    api_key = os.getenv("CFBD_API_KEY")
    if not api_key:
        raise RuntimeError("CFBD_API_KEY not found in environment variables or .env file.")
    headers = {"Authorization": f"Bearer {api_key}"}
    # Fetch all FBS teams for the given year
    teams_url = f"https://api.collegefootballdata.com/teams/fbs?year={args.year}"
    print(f"Fetching FBS teams for {args.year} from {teams_url} ...")
    try:
        teams_response = requests.get(teams_url, headers=headers)
        teams_response.raise_for_status()
        teams = teams_response.json()
    except Exception as e:
        print("Failed to fetch teams:", e)
        return
    if not teams:
        print("No teams data returned.")
        return

    # Fetch roster for each team
    all_players = []
    for team in teams:
        team_name = team.get("school")
        if not team_name:
            continue
        roster_url = f"https://api.collegefootballdata.com/roster?year={args.year}&team={requests.utils.quote(team_name)}"
        print(f"Fetching roster for {team_name} ...")
        try:
            roster_response = requests.get(roster_url, headers=headers)
            if roster_response.status_code == 429:
                print(f"Rate limit hit for {team_name}. Waiting 10 seconds and retrying...")
                time.sleep(10)
                roster_response = requests.get(roster_url, headers=headers)
            roster_response.raise_for_status()
            players = roster_response.json()
            if players:
                for player in players:
                    player["team"] = team_name  # Ensure team name is present
                all_players.extend(players)
        except Exception as e:
            print(f"Failed to fetch roster for {team_name}: {e}")
            continue
        time.sleep(1.5)  # Sleep between requests to avoid rate limiting

    if not all_players:
        print("No roster data returned for any team.")
        return

    # Save as CSV in data/rosters
    rosters_dir = os.path.join(args.output_dir, "rosters")
    os.makedirs(rosters_dir, exist_ok=True)
    csv_path = os.path.join(rosters_dir, f"{args.year}_ncaa_roster.csv")
    df = pd.DataFrame(all_players)
    df.to_csv(csv_path, index=False)
    print(f"Saved {csv_path}")

if __name__ == "__main__":
    main()
