import csv
import firebase_admin
from firebase_admin import credentials, firestore
from pathlib import Path
import ast
import json

def initialize_firebase():
    current_dir = Path(__file__).resolve().parent
    cred = credentials.Certificate(current_dir / "serviceAccountKey.json")
    firebase_admin.initialize_app(cred)
    return firestore.client()

def parse_complex_field(field):
    try:
        return ast.literal_eval(field)
    except:
        return field

def read_running_backs_data(file_path):
    running_backs = []
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                name = parse_complex_field(row['name'])
                percent_owned = parse_complex_field(row['ownership_percentage'])
                headshot = parse_complex_field(row['headshot'])
                bye_weeks = parse_complex_field(row['bye_weeks'])

                rb = {
                    'name': name['full'] if isinstance(name, dict) else str(name),
                    'player_id': row['player_id'],
                    'player_key': row['player_key'],
                    'uniform_number': row.get('uniform_number') or '',
                    'percent_owned': float(percent_owned['value']) if isinstance(percent_owned, dict) and 'value' in percent_owned else 0,
                    'image_url': headshot['url'] if isinstance(headshot, dict) and 'url' in headshot else '',
                    'injury_note': row.get('injury_note', ''),
                    'position': 'RB',
                    'team_abbr': row['editorial_team_abbr'],
                    'team_full_name': row['editorial_team_full_name'],
                    'bye_week': bye_weeks['week'] if isinstance(bye_weeks, dict) and 'week' in bye_weeks else None,
                    'status': row.get('status', ''),
                    'status_full': row.get('status_full', '')
                }
                running_backs.append(rb)
            except Exception as e:
                print(f"Error processing row: {e}")
                print("Row data:", row)
    
    print(f"Processed {len(running_backs)} running backs")
    return running_backs

def update_games_with_rbs(db, running_backs):
    games_ref = db.collection('nfl_schedule')
    games = games_ref.get()

    for game in games:
        game_data = game.to_dict()
        home_team = game_data['home_team']
        away_team = game_data['visitor_team']

        home_rbs = [rb for rb in running_backs if rb['team_full_name'] == home_team]
        away_rbs = [rb for rb in running_backs if rb['team_full_name'] == away_team]

        # Sort RBs by percent owned, descending
        home_rbs.sort(key=lambda x: x['percent_owned'], reverse=True)
        away_rbs.sort(key=lambda x: x['percent_owned'], reverse=True)

        # Update game document
        game.reference.set({
            'home_rbs': home_rbs,
            'away_rbs': away_rbs
        }, merge=True)

        print(f"Updated game: {away_team} @ {home_team}")

def main():
    db = initialize_firebase()
    current_dir = Path(__file__).resolve().parent
    rb_data_path = current_dir / 'running_backs_ownership_2024.csv'
    
    running_backs = read_running_backs_data(rb_data_path)
    if running_backs:
        update_games_with_rbs(db, running_backs)
        print("All games updated with running backs information.")
    else:
        print("No running backs data processed. Please check the CSV file.")

if __name__ == "__main__":
    main()