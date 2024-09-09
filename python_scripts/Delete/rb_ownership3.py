import os
import sys
import csv
from pathlib import Path
import time

# Add the parent directory to Python path
current_dir = Path(__file__).resolve().parent
project_dir = current_dir.parent
sys.path.insert(0, str(project_dir))

from yfpy.query import YahooFantasySportsQuery
from yfpy.logger import get_logger
from logging import DEBUG

# Load environment variables
from dotenv import load_dotenv
load_dotenv(dotenv_path=project_dir / "auth" / ".env")

# Set directory location of private.json for authentication
auth_dir = project_dir / "auth"

# Constants
LEAGUE_ID = '935667'  # Your league ID
GAME_CODE = 'nfl'
SEASON = 2024  # Set this to the desired season year
PLAYERS_PER_QUERY = 25  # Number of players to fetch per API call

# Configure logger
logger = get_logger("rb_ownership_script", DEBUG)

# YahooFantasySportsQuery setup
yahoo_query = YahooFantasySportsQuery(
    auth_dir,
    LEAGUE_ID,
    GAME_CODE,
    offline=False,
    all_output_as_json_str=False,
    consumer_key=os.environ["YFPY_CONSUMER_KEY"],
    consumer_secret=os.environ["YFPY_CONSUMER_SECRET"]
)

def get_game_id_for_season(season):
    logger.info(f"Fetching game ID for NFL {season} season...")
    game_id = yahoo_query.get_game_key_by_season(season)
    logger.info(f"Game ID for NFL {season} season: {game_id}")
    return game_id

def refresh_token():
    logger.info("Attempting to refresh OAuth token...")
    yahoo_query.yahoo_session.oauth.refresh_access_token()
    logger.info("Token refreshed successfully.")

def get_all_players(game_id):
    yahoo_query.game_id = game_id
    yahoo_query.league_key = f"{game_id}.l.{LEAGUE_ID}"

    all_players = []
    start = 0
    
    while True:
        logger.info(f"Fetching players {start} to {start + PLAYERS_PER_QUERY}...")
        players_chunk = yahoo_query.get_league_players(player_count_limit=PLAYERS_PER_QUERY, player_count_start=start)
        
        if not players_chunk:
            break
        
        all_players.extend(players_chunk)
        start += PLAYERS_PER_QUERY
        
        # Add a small delay to avoid hitting rate limits
        time.sleep(1)
    
    logger.info(f"Total players fetched: {len(all_players)}")
    return all_players

def is_running_back(player):
    positions = player.get('eligible_positions', []) if isinstance(player, dict) else getattr(player, 'eligible_positions', [])
    return 'RB' in positions

def get_player_key(player):
    return player.get('player_key', getattr(player, 'player_key', None))

def get_player_name(player):
    if isinstance(player, dict):
        return player.get('name', {}).get('full', 'Unknown')
    return getattr(player, 'name', {}).full if hasattr(player, 'name') else 'Unknown'

def get_rb_ownership_data(game_id):
    all_players = get_all_players(game_id)
    running_backs = [player for player in all_players if is_running_back(player)]
    logger.info(f"Number of running backs: {len(running_backs)}")

    rb_ownership_data = []
    for i, rb in enumerate(running_backs, 1):
        player_key = get_player_key(rb)
        player_name = get_player_name(rb)
        
        if player_key:
            try:
                ownership_data = yahoo_query.get_player_ownership(player_key)
                ownership_percentage = ownership_data.get('percent_owned', 0)
                rb_ownership_data.append([player_name, ownership_percentage])
                logger.info(f"Processed {i}/{len(running_backs)}: {player_name} - Ownership: {ownership_percentage}%")
            except Exception as e:
                logger.error(f"Error fetching ownership data for {player_name}: {e}")
        else:
            logger.warning(f"Could not process player {player_name}: No player key found")
        
        # Add a small delay to avoid hitting rate limits
        time.sleep(0.5)

    return rb_ownership_data

def save_to_csv(data, filename="running_back_ownership_2024.csv"):
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Player Name', 'Ownership Percentage'])
        writer.writerows(data)
    logger.info(f"Data saved to {filename}")

if __name__ == "__main__":
    logger.info("Starting running back ownership query for 2024 NFL season...")
    try:
        game_id = get_game_id_for_season(SEASON)
        rb_data = get_rb_ownership_data(game_id)
        
        # Sort the data by ownership percentage (descending order)
        rb_data.sort(key=lambda x: float(x[1]), reverse=True)
        
        save_to_csv(rb_data)
        logger.info("Query completed.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.info("Please check your league ID, authentication credentials, and network connection.")