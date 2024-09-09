import firebase_admin
from firebase_admin import credentials, firestore
from pathlib import Path
import json
from datetime import datetime

def initialize_firebase():
    current_dir = Path(__file__).resolve().parent
    cred = credentials.Certificate(current_dir / "serviceAccountKey.json")
    firebase_admin.initialize_app(cred)
    return firestore.client()

def read_running_backs_data(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Ensure ownership_percentage is a float
    for rb in data:
        ownership_percentage = rb.get('ownership_percentage', 0)
        rb['ownership_percentage'] = float(ownership_percentage)
    
    print(f"Sample RB data: {json.dumps(data[0], indent=2)}")
    return data

def simplify_rb_data(rb):
    return {
        'name': rb.get('full_name', rb.get('name', 'Unknown')),
        'team': rb.get('editorial_team_full_name', 'Unknown Team'),
        'percent_owned': float(rb.get('ownership_percentage', 0)),
        'status': rb.get('status', 'Active'),
        'status_full': rb.get('status_full', 'Active'),
        'bye': rb.get('bye', None),
        'headshot_url': rb.get('headshot_url', ''),
        'injury_note': rb.get('injury_note', '')
    }

def datetime_to_dict(dt):
    if isinstance(dt, datetime):
        return {
            "year": dt.year,
            "month": dt.month,
            "day": dt.day,
            "hour": dt.hour,
            "minute": dt.minute,
            "second": dt.second,
            "microsecond": dt.microsecond
        }
    return str(dt)

def update_games_with_rbs(db, running_backs):
    windows_ref = db.collection('game_windows')
    windows = windows_ref.get()

    for window in windows:
        window_data = window.to_dict()
        updated_games = []

        for game in window_data['games']:
            home_team = game['home_team']
            away_team = game['visitor_team']

            home_rbs = [rb for rb in running_backs if rb['editorial_team_full_name'] == home_team]
            away_rbs = [rb for rb in running_backs if rb['editorial_team_full_name'] == away_team]

            home_rbs.sort(key=lambda x: float(x.get('ownership_percentage', 0)), reverse=True)
            away_rbs.sort(key=lambda x: float(x.get('ownership_percentage', 0)), reverse=True)

            game['home_rbs'] = [simplify_rb_data(rb) for rb in home_rbs]
            game['away_rbs'] = [simplify_rb_data(rb) for rb in away_rbs]
            
            # Convert datetime fields to a serializable format
            game['datetime'] = datetime_to_dict(game['datetime'])
            
            updated_games.append(game)

        # Update window document
        window.reference.update({
            'games': updated_games
        })

        print(f"Updated window: {window.id}")
        print(f"Number of games in this window: {len(updated_games)}")
        print(f"Sample game data: {json.dumps(updated_games[0], indent=2, default=str)}")

def main():
    db = initialize_firebase()
    current_dir = Path(__file__).resolve().parent
    rb_data_path = current_dir / 'running_backs_ownership_2024.json'
    
    running_backs = read_running_backs_data(rb_data_path)
    if running_backs:
        print(f"Number of running backs loaded: {len(running_backs)}")
        print(f"Sample RB data: {json.dumps(running_backs[0], indent=2)}")
        update_games_with_rbs(db, running_backs)
        print("All game windows updated with running backs information.")
    else:
        print("No running backs data processed. Please check the JSON file.")

if __name__ == "__main__":
    main()