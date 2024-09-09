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
PLAYERS_PER_QUERY = 3500  # Number of players to fetch per API call
MAX_RETRIES = 3  # Maximum number of retries for API calls

# Configure logger
logger = get_logger("all_players_download", DEBUG)

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

def get_all_players(game_id):
    yahoo_query.game_id = game_id
    yahoo_query.league_key = f"{game_id}.l.{LEAGUE_ID}"

    all_players = []
    start = 0
    retries = 0
    
    while retries < MAX_RETRIES:
        logger.info(f"Fetching players {start} to {start + PLAYERS_PER_QUERY}...")
        try:
            players_chunk = yahoo_query.get_league_players(player_count_limit=PLAYERS_PER_QUERY, player_count_start=start)
            
            if not players_chunk:
                logger.info(f"No more players returned after {start} players.")
                break
            
            logger.debug(f"Players in this chunk: {len(players_chunk)}")
            logger.debug(f"First player in chunk: {players_chunk[0]}")
            
            all_players.extend(players_chunk)
            start += PLAYERS_PER_QUERY
            retries = 0  # Reset retries on successful fetch
            
            # Add a small delay to avoid hitting rate limits
            time.sleep(1)
        except Exception as e:
            logger.error(f"Error fetching players: {e}")
            retries += 1
            logger.info(f"Retrying... (Attempt {retries} of {MAX_RETRIES})")
            time.sleep(5)  # Wait a bit longer before retrying
    
    logger.info(f"Total players fetched: {len(all_players)}")
    return all_players

def player_to_dict(player):
    if isinstance(player, dict):
        return player
    return {attr: getattr(player, attr) for attr in dir(player) if not attr.startswith('_')}

def save_players_to_csv(players, filename="all_players_2024.csv"):
    if not players:
        logger.warning("No players to save.")
        return

    # Get all unique keys from all player dictionaries
    all_keys = set()
    for player in players:
        all_keys.update(player_to_dict(player).keys())

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=sorted(all_keys))
        writer.writeheader()
        
        for player in players:
            player_dict = player_to_dict(player)
            # Ensure all keys are present, use None for missing values
            row = {key: player_dict.get(key, None) for key in all_keys}
            writer.writerow(row)
    
    logger.info(f"All player data saved to {filename}")

if __name__ == "__main__":
    logger.info(f"Starting download of all NFL players for {SEASON} season...")
    try:
        game_id = get_game_id_for_season(SEASON)
        all_players = get_all_players(game_id)
        save_players_to_csv(all_players)
        logger.info("Player download and CSV creation completed.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.info("Please check your league ID, authentication credentials, and network connection.")