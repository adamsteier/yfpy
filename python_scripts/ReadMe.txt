UPDATE EVERY YEAR
Change nfl_schedule.csv to current YEAR
run firebase_schedule_uploader.py to update the schedule in the firestore


Script should work by running download_all_players.py once per week.  This will get all players that have been activated
in the leage.

filter_running_backs.py will filter out any players that are not running backs and then get the ownership for each of the RBs.

It will then save the data to a csv file running_back_ownership.csv which can then be used to see the ownership of each RB.
