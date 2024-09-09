import csv
from datetime import datetime, timedelta
from collections import defaultdict
import firebase_admin
from firebase_admin import credentials, firestore
from pathlib import Path

def initialize_firebase():
    current_dir = Path(__file__).resolve().parent
    cred = credentials.Certificate(current_dir / "serviceAccountKey.json")
    firebase_admin.initialize_app(cred)
    return firestore.client()

def parse_date_time(date_str, time_str, year):
    full_date_str = f"{date_str} {year} {time_str}"
    date_obj = datetime.strptime(full_date_str, "%B %d %Y %I:%M %p")
    return date_obj.replace(tzinfo=None) - timedelta(hours=4)

def process_schedule(file_path):
    schedule = []
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row
        for row in reader:
            game = {
                'week': int(row[0]),
                'day': row[1],
                'date': row[2],
                'visitor_team': row[3],
                'home_team': row[6],
                'time': row[8]
            }
            schedule.append(game)
    return schedule

def group_games_into_windows(schedule):
    windows = defaultdict(list)
    current_year = datetime.now().year
    
    for game in schedule:
        game_datetime = parse_date_time(game['date'], game['time'], current_year)
        window_key = game_datetime.strftime("%Y-%m-%d %H:%M")
        
        game_data = {
            'week': game['week'],
            'day': game['day'],
            'datetime': game_datetime,
            'visitor_team': game['visitor_team'],
            'home_team': game['home_team']
        }
        
        windows[window_key].append(game_data)
    
    return windows

def upload_to_firebase(db, windows):
    for window_key, games in windows.items():
        try:
            window_data = {
                'datetime': games[0]['datetime'],
                'games': games,
                'timestamp': firestore.SERVER_TIMESTAMP
            }
            
            doc_id = f"window_{window_key.replace(' ', '_')}"
            db.collection('game_windows').document(doc_id).set(window_data)
            print(f"Uploaded window: {window_key} with {len(games)} games")
        except Exception as e:
            print(f"Error processing window: {window_key}. Error: {e}")

def main():
    db = initialize_firebase()
    current_dir = Path(__file__).resolve().parent
    schedule_path = current_dir / 'nfl_schedule.csv'
    schedule = process_schedule(schedule_path)
    windows = group_games_into_windows(schedule)
    upload_to_firebase(db, windows)
    print("Schedule upload completed.")

if __name__ == "__main__":
    main()