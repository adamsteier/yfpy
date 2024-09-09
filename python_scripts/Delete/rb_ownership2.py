import os
import sys
import csv
from pathlib import Path

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

def get_rb_ownership_data(game_id):
    yahoo_query.game_id = game_id  # Update the game_id in the query object
    yahoo_query.league_key = f"{game_id}.l.{LEAGUE_ID}"  # Update the league_key

    try:
        logger.info("Fetching all players...")
        all_players = yahoo_query.get_league_players()
        logger.info(f"Total players fetched: {len(all_players)}")
        
        # Debug: Print the type and structure of the first player
        if all_players:
            logger.debug(f"Type of first player: {type(all_players[0])}")
            logger.debug(f"First player data: {all_players[0]}")
    except Exception as e:
        logger.error(f"Error fetching players: {e}")
        logger.info("Attempting to refresh token and retry...")
        refresh_token()
        all_players = yahoo_query.get_league_players()
        logger.info(f"Total players fetched after token refresh: {len(all_players)}")

    running_backs = []
    for player in all_players:
        try:
            if isinstance(player, dict):
                if 'RB' in player.get('eligible_positions', []):
                    running_backs.append(player)
            elif hasattr(player, 'eligible_positions'):
                if 'RB' in player.eligible_positions:
                    running_backs.append(player)
            else:
                logger.warning(f"Unexpected player data type: {type(player)}")
        except Exception as e:
            logger.error(f"Error processing player: {e}")

    logger.info(f"Number of running backs: {len(running_backs)}")

    rb_ownership_data = []
    for i, rb in enumerate(running_backs, 1):
        try:
            player_key = rb.get('player_key', rb.player_key if hasattr(rb, 'player_key') else None)
            player_name = rb.get('name', {}).get('full', rb.name if hasattr(rb, 'name') else 'Unknown')
            
            if player_key:
                ownership_data = yahoo_query.get_player_ownership(player_key)
                ownership_percentage = ownership_data.get('percent_owned', 0)
                rb_ownership_data.append([player_name, ownership_percentage])
                logger.info(f"Processed {i}/{len(running_backs)}: {player_name} - Ownership: {ownership_percentage}%")
            else:
                logger.warning(f"Could not process player {player_name}: No player key found")
        except Exception as e:
            logger.error(f"Error fetching ownership data: {e}")

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