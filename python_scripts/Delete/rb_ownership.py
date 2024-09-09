import os
import csv
import sys
from pathlib import Path

from dotenv import load_dotenv

# Add yfpy to Python path
yfpy_dir = Path(__file__).parent.parent
sys.path.insert(0, str(yfpy_dir))

from yfpy.query import YahooFantasySportsQuery

# Load environment variables
load_dotenv()

# Constants
LEAGUE_ID = '935667'
GAME_CODE = 'nfl'
GAME_ID = 423
YFPY_APP_ID = 'zncpK6vg'

# Authentication
auth_dir = Path(__file__).parent.parent / "auth"
auth_dir_str = str(auth_dir).replace("/", "\\") 

# Set the PRIVATE_JSON_PATH environment variable
os.environ['PRIVATE_JSON_PATH'] = os.path.join(auth_dir_str, "private.json")

# YahooFantasySportsQuery setup
yahoo_query = YahooFantasySportsQuery(
    auth_dir_str,
    LEAGUE_ID,
    GAME_CODE,
    game_id=GAME_ID,
    offline=False,
    all_output_as_json_str=False,
    consumer_key=os.getenv("YFPY_CONSUMER_KEY"),
    consumer_secret=os.getenv("YFPY_CONSUMER_SECRET")
)

# Fetch and process data
all_players = yahoo_query.get_league_players()
running_backs = [player for player in all_players if 'RB' in player['eligible_positions']]

rb_ownership_data = []
for rb in running_backs:
    player_key = rb['player_key']
    ownership_data = yahoo_query.get_player_ownership(player_key)
    ownership_percentage = ownership_data['percent_owned']
    rb_ownership_data.append([rb['name']['full'], ownership_percentage])

# Sort the data by ownership percentage (descending order)
rb_ownership_data.sort(key=lambda x: x[1], reverse=True)

# Save to CSV
with open('running_back_ownership.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Player Name', 'Ownership Percentage'])
    writer.writerows(rb_ownership_data)

print("Running back ownership data saved to 'running_back_ownership.csv'")