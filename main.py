from flask import Flask, render_template, request
from nba_api.stats.endpoints import playercareerstats, playerawards, commonplayerinfo
from nba_api.stats.static import players
import pandas as pd
from datetime import datetime
from db_handler import NBADatabase

app = Flask(__name__)

def get_player_stats(player_name):
    player = players.find_players_by_full_name(player_name)
    
    if not player:
        return None, None, None, None, None, None, None, None, None, "No player found with that name."
    
    player_id = player[0]['id']
    player_image_url = f"https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/{player_id}.png"
    
    # Initialize database connection
    db = NBADatabase()
    
    try:
        # Get all player data and check if it needs updating
        stored_data = db.get_player_data(player_id)
        
        if stored_data and not db.needs_update(stored_data):
            # Return data in the format expected by the template
            return (
                stored_data['player_details']['image_url'],
                stored_data['player_details'],
                pd.DataFrame(stored_data['season_stats']),
                pd.DataFrame(stored_data['season_averages']),
                pd.DataFrame([stored_data['career_totals']]) if stored_data['career_totals'] else None,
                pd.DataFrame([stored_data['career_averages']]) if stored_data['career_averages'] else None,
                pd.DataFrame(stored_data['playoff_stats']) if stored_data['playoff_stats'] else None,
                pd.DataFrame(stored_data['playoff_averages']) if stored_data['playoff_averages'] else None,
                stored_data['awards'],
                None
            )

        # If data doesn't exist or needs updating, fetch from API
        # Get detailed player info
        player_info = commonplayerinfo.CommonPlayerInfo(player_id)
        info_df = player_info.get_data_frames()[0]
        
        # Create player info dictionary
        player_details = {
            'Name': info_df['DISPLAY_FIRST_LAST'].iloc[0],
            'Position': info_df['POSITION'].iloc[0],
            'Height': info_df['HEIGHT'].iloc[0],
            'Weight': f"{info_df['WEIGHT'].iloc[0]} lbs",
            'Birth_Date': datetime.strptime(info_df['BIRTHDATE'].iloc[0], '%Y-%m-%dT%H:%M:%S').strftime('%B %d, %Y'),
            'Country': info_df['COUNTRY'].iloc[0],
            'Experience': f"{info_df['SEASON_EXP'].iloc[0]} years",
            'Jersey': f"#{info_df['JERSEY'].iloc[0]}" if not pd.isna(info_df['JERSEY'].iloc[0]) else "N/A",
            'Team': f"{info_df['TEAM_CITY'].iloc[0]} {info_df['TEAM_NAME'].iloc[0]}" if not pd.isna(info_df['TEAM_CITY'].iloc[0]) else "N/A"
        }
        
        # Get player awards
        player_awards = playerawards.PlayerAwards(player_id)
        awards_df = player_awards.get_data_frames()[0]
        
        # Group awards by type and count them
        if not awards_df.empty:
            award_counts = awards_df['DESCRIPTION'].value_counts().sort_values(ascending=False)
            # Convert to list of strings
            awards_list = [f"{count}x {award}" for award, count in award_counts.items()]
        else:
            awards_list = None

        # Get stats using the correct method
        career_stats = playercareerstats.PlayerCareerStats(player_id)
        
        # Get different types of stats
        career_totals_regular = career_stats.career_totals_regular_season.get_data_frame()
        career_totals_playoffs = career_stats.career_totals_post_season.get_data_frame()
        season_totals_regular = career_stats.season_totals_regular_season.get_data_frame()
        season_totals_playoffs = career_stats.season_totals_post_season.get_data_frame()
        
        # Drop Team-ID from career totals specifically
        if not career_totals_regular.empty and 'Team_ID' in career_totals_regular.columns:
            career_totals_regular = career_totals_regular.drop(columns=['Team_ID'])
        
        # Drop unwanted columns and format percentages for totals tables
        columns_to_drop = ['TEAM_ID', 'LEAGUE_ID', 'PLAYER_ID']
        for df in [career_totals_regular, career_totals_playoffs, season_totals_regular, season_totals_playoffs]:
            if not df.empty:
                df.drop(columns=columns_to_drop, errors='ignore', inplace=True)
                # Convert PLAYER_AGE to integer if it exists
                if 'PLAYER_AGE' in df.columns:
                    df['PLAYER_AGE'] = df['PLAYER_AGE'].astype(int)
                
                # Format percentages for totals tables
                pct_columns = ['FG_PCT', 'FG3_PCT', 'FT_PCT']
                for col in pct_columns:
                    if col in df.columns:
                        df[col] = ((df[col] * 100).round(1)).astype(str) + '%'
                
                # Round all other numeric columns to 1 decimal
                numeric_columns = df.select_dtypes(include=['float64']).columns
                df[numeric_columns] = df[numeric_columns].round(1)
        
        # Calculate per-game averages for regular season and playoffs
        per_game_stats = None
        playoff_per_game_stats = None
        
        if not season_totals_regular.empty:
            per_game_stats = season_totals_regular.copy()  # Keep original stats for DB
            display_per_game_stats = season_totals_regular.copy()  # Create copy for display
            
            # Calculate averages for display only
            stats_to_average = ['PTS', 'REB', 'AST', 'MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 
                              'FTM', 'FTA', 'OREB', 'DREB', 'STL', 'BLK', 'TOV', 'PF']
            
            for stat in stats_to_average:
                if stat in display_per_game_stats.columns:
                    display_per_game_stats[stat] = display_per_game_stats[stat] / display_per_game_stats['GP']
            
            # Round for display
            numeric_columns = display_per_game_stats.select_dtypes(include=['float64']).columns
            display_per_game_stats[numeric_columns] = display_per_game_stats[numeric_columns].round(1)

        # Calculate playoff per-game averages
        if not season_totals_playoffs.empty:
            playoff_per_game_stats = season_totals_playoffs.copy()  # Keep original stats for DB
            display_playoff_per_game_stats = season_totals_playoffs.copy()  # Create copy for display
            
            for stat in stats_to_average:
                if stat in display_playoff_per_game_stats.columns:
                    display_playoff_per_game_stats[stat] = display_playoff_per_game_stats[stat] / display_playoff_per_game_stats['GP']
            
            numeric_columns = display_playoff_per_game_stats.select_dtypes(include=['float64']).columns
            display_playoff_per_game_stats[numeric_columns] = display_playoff_per_game_stats[numeric_columns].round(1)

        # Calculate career averages only (since we already have career_totals_regular)
        if not career_totals_regular.empty:
            total_games = career_totals_regular['GP'].values[0]  # Get GP from career totals
            career_averages = pd.DataFrame({
                'GP': [total_games],
                'PTS': [career_totals_regular['PTS'].values[0] / total_games],
                'REB': [career_totals_regular['REB'].values[0] / total_games],
                'AST': [career_totals_regular['AST'].values[0] / total_games],
                'STL': [career_totals_regular['STL'].values[0] / total_games],
                'BLK': [career_totals_regular['BLK'].values[0] / total_games]
            }, index=['Career Averages'])
            
            career_averages = career_averages.round(1)
        else:
            career_averages = None

        # Update database with raw stats
        db.upsert_player(player_details, player_id)
        db.upsert_career_totals(career_totals_regular, player_id)
        db.upsert_career_averages(career_totals_regular, player_id)
        db.upsert_regular_season_totals(season_totals_regular, player_id)
        db.upsert_playoff_totals(season_totals_playoffs, player_id)
        db.upsert_regular_season_averages(season_totals_regular, player_id)  # Pass raw stats
        db.upsert_playoff_averages(season_totals_playoffs, player_id)  # Pass raw stats
        db.upsert_awards(awards_list, player_id)
        
        return (player_image_url, player_details, season_totals_regular, display_per_game_stats,
                career_totals_regular, career_averages, season_totals_playoffs,
                display_playoff_per_game_stats, awards_list, None)
    
    finally:
        db.close()

@app.route('/', methods=['GET', 'POST'])
def index():
    stats = None
    per_game_stats = None
    career_totals = None
    career_averages = None
    player_image_url = None
    playoff_stats = None
    playoff_per_game_stats = None
    awards_list = None
    player_details = None
    error_message = None
    
    if request.method == 'POST':
        player_name = request.form.get('player_name')
        (player_image_url, player_details, stats, per_game_stats, career_totals, career_averages, 
         playoff_stats, playoff_per_game_stats, awards_list, error_message) = get_player_stats(player_name)
    
    return render_template('index.html', 
                         stats=create_sortable_table(stats) if stats is not None else None,
                         per_game_stats=create_sortable_table(per_game_stats) if per_game_stats is not None else None,
                         career_totals=create_sortable_table(career_totals) if career_totals is not None else None,
                         career_averages=career_averages.to_html(classes='table table-striped') if career_averages is not None else None,
                         player_image_url=player_image_url,
                         playoff_stats=create_sortable_table(playoff_stats) if playoff_stats is not None else None,
                         playoff_per_game_stats=create_sortable_table(playoff_per_game_stats) if playoff_per_game_stats is not None else None,
                         awards_list=awards_list,
                         player_details=player_details,
                         error_message=error_message)

def create_sortable_table(df, classes='table table-striped', index=False):
    """Create an HTML table with sortable columns"""
    html = df.to_html(classes=classes, index=index)
    # Add sort buttons to headers
    html = html.replace('<th>', '<th class="sortable" onclick="sortTable(this)">')
    return html

if __name__ == '__main__':
    app.run(debug=True)

