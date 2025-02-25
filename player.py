from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.static import players
import pandas as pd

def get_player_stats(player_name):
    # Get player info by name
    player = players.find_players_by_full_name(player_name)
    
    if not player:
        print(f"No player found with name: {player_name}")
        return None
    
    # Get the first matching player's ID
    player_id = player[0]['id']
    
    # Get career stats
    career = playercareerstats.PlayerCareerStats(player_id=player_id)
    
    # Get data frames, json, and dictionary
    data_frame = career.get_data_frames()[0]
    json_data = career.get_json()
    dict_data = career.get_dict()
    
    return data_frame, json_data, dict_data

# Get player name from terminal input
player_name = input("Enter the player's name: ")  # User input for player name
stats = get_player_stats(player_name)

if stats:
    data_frame, json_data, _ = stats
    print(f"\nCareer stats for {player_name}:")
    print(data_frame)
    print("\nJSON format:")
    print(json_data)


