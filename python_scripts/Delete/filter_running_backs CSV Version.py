import pandas as pd
import os
import sys
from pathlib import Path
import time
from tqdm import tqdm

# Add the parent directory to Python path
current_dir = Path(__file__).resolve().parent
project_dir = current_dir.parent
sys.path.insert(0, str(project_dir))

from yfpy.query import YahooFantasySportsQuery
from yfpy.logger import get_logger
from logging import DEBUG
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path=project_dir / "auth" / ".env")

# Set directory location of private.json for authentication
auth_dir = project_dir / "auth"

# Constants
LEAGUE_ID = '935667'  # Your league ID
GAME_CODE = 'nfl'
SEASON = 2024  # Set this to the desired season year
CURRENT_WEEK = 1  # Set this to the current week
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Configure logger
logger = get_logger("rb_ownership_script", DEBUG)

# YahooFantasySportsQuery setup
yahoo_query = YahooFantasySportsQuery(
    auth_dir,
    LEAGUE_ID,
    GAME_CODE,
    game_id=None,  # We'll set this later
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

def get_ownership_percentage(player_key, week):
    for attempt in range(MAX_RETRIES):
        try:
            player_data = yahoo_query.get_player_percent_owned_by_week(player_key, week)
            logger.info(f"Raw player data for {player_key}: {player_data}")
            
            if hasattr(player_data, 'percent_owned'):
                if isinstance(player_data.percent_owned, dict) and 'value' in player_data.percent_owned:
                    return player_data.percent_owned['value']
                else:
                    return player_data.percent_owned
            elif isinstance(player_data, dict):
                if 'percent_owned' in player_data:
                    if isinstance(player_data['percent_owned'], dict) and 'value' in player_data['percent_owned']:
                        return player_data['percent_owned']['value']
                    else:
                        return player_data['percent_owned']
            
            logger.warning(f"No percent_owned attribute or key for player {player_key}")
            return 0
        except Exception as e:
            logger.error(f"Error fetching ownership data for player {player_key}: {e}")
            if "rate limiting" in str(e).lower():
                logger.info(f"Rate limit hit. Waiting {RETRY_DELAY} seconds before retry...")
                time.sleep(RETRY_DELAY)
            else:
                return 0
        if attempt < MAX_RETRIES - 1:
            time.sleep(1)  # Add a small delay between retries
    return 0

def process_running_backs(input_file='all_players_2024.csv', output_file='running_backs_ownership_2024.csv'):
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        logger.error(f"Input file {input_file} not found.")
        return
    except pd.errors.EmptyDataError:
        logger.error(f"Input file {input_file} is empty.")
        return
    except Exception as e:
        logger.error(f"Error reading input file {input_file}: {e}")
        return

    # Filter for running backs
    rb_df = df[df['primary_position'] == 'RB'].copy()

    if rb_df.empty:
        logger.warning("No running backs found in the input file.")
        return

    # Get the game ID for the current season
    game_id = get_game_id_for_season(SEASON)
    yahoo_query.game_id = game_id

    # Add ownership percentage column
    tqdm.pandas(desc="Fetching ownership percentages")
    rb_df['ownership_percentage'] = rb_df['player_key'].progress_apply(
        lambda x: get_ownership_percentage(x, CURRENT_WEEK)
    )

    # Try to save the CSV file
    try:
        rb_df.to_csv(output_file, index=False)
        logger.info(f"Running backs with ownership percentages have been saved to {output_file}")
    except PermissionError:
        logger.error(f"Permission denied to save at {output_file}. Please close any programs that might be using this file and try again.")
    except Exception as e:
        logger.error(f"Error saving output file {output_file}: {e}")

    logger.info(f"Total number of running backs: {len(rb_df)}")
    logger.info(f"Number of running backs with non-zero ownership: {(rb_df['ownership_percentage'] > 0).sum()}")
    logger.info(f"Average ownership percentage: {rb_df['ownership_percentage'].mean():.2f}%")
    logger.info(f"Max ownership percentage: {rb_df['ownership_percentage'].max():.2f}%")

if __name__ == "__main__":
    process_running_backs()