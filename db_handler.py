import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

class NBADatabase:
    def __init__(self):
        self.conn = psycopg2.connect(DATABASE_URL)
        
    def upsert_player(self, player_details, player_id):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO players (
                    player_id, name, position, height, weight, 
                    birth_date, country, experience, jersey_number, team, image_url
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id) 
                DO UPDATE SET
                    position = EXCLUDED.position,
                    height = EXCLUDED.height,
                    weight = EXCLUDED.weight,
                    experience = EXCLUDED.experience,
                    jersey_number = EXCLUDED.jersey_number,
                    team = EXCLUDED.team,
                    image_url = EXCLUDED.image_url,
                    last_updated = CURRENT_TIMESTAMP
            """, (
                player_id,
                player_details['Name'],
                player_details['Position'],
                player_details['Height'],
                player_details['Weight'].replace(' lbs', ''),
                datetime.strptime(player_details['Birth_Date'], '%B %d, %Y'),
                player_details['Country'],
                int(player_details['Experience'].replace(' years', '')),
                player_details['Jersey'].replace('#', ''),
                player_details['Team'],
                f"https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/{player_id}.png"
            ))
            self.conn.commit()

    def upsert_career_totals(self, career_totals, player_id):
        if career_totals.empty:
            return
            
        data = career_totals.iloc[0]
        
        # Convert numpy types to Python native types
        values = (
            int(player_id),
            int(data['GP']),
            float(data['PTS']),
            float(data['REB']),
            float(data['AST']),
            float(data['STL']),
            float(data['BLK']),
            float(data['MIN']),
            float(data['FGM']),
            float(data['FGA']),
            float(str(data['FG_PCT']).rstrip('%')) / 100 if pd.notna(data['FG_PCT']) else None,
            float(data['FG3M']),
            float(data['FG3A']),
            float(str(data['FG3_PCT']).rstrip('%')) / 100 if pd.notna(data['FG3_PCT']) else None,
            float(data['FTM']),
            float(data['FTA']),
            float(str(data['FT_PCT']).rstrip('%')) / 100 if pd.notna(data['FT_PCT']) else None,
            float(data['OREB']),
            float(data['DREB']),
            float(data['TOV']),
            float(data['PF'])
        )
        
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO career_totals (
                    player_id, games_played, points, rebounds, assists,
                    steals, blocks, minutes, field_goals_made, field_goals_attempted,
                    fg_percentage, three_points_made, three_points_attempted,
                    fg3_percentage, free_throws_made, free_throws_attempted,
                    ft_percentage, offensive_rebounds, defensive_rebounds,
                    turnovers, personal_fouls
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (player_id) DO UPDATE SET
                    games_played = EXCLUDED.games_played,
                    points = EXCLUDED.points,
                    rebounds = EXCLUDED.rebounds,
                    assists = EXCLUDED.assists,
                    steals = EXCLUDED.steals,
                    blocks = EXCLUDED.blocks,
                    minutes = EXCLUDED.minutes,
                    field_goals_made = EXCLUDED.field_goals_made,
                    field_goals_attempted = EXCLUDED.field_goals_attempted,
                    fg_percentage = EXCLUDED.fg_percentage,
                    three_points_made = EXCLUDED.three_points_made,
                    three_points_attempted = EXCLUDED.three_points_attempted,
                    fg3_percentage = EXCLUDED.fg3_percentage,
                    free_throws_made = EXCLUDED.free_throws_made,
                    free_throws_attempted = EXCLUDED.free_throws_attempted,
                    ft_percentage = EXCLUDED.ft_percentage,
                    offensive_rebounds = EXCLUDED.offensive_rebounds,
                    defensive_rebounds = EXCLUDED.defensive_rebounds,
                    turnovers = EXCLUDED.turnovers,
                    personal_fouls = EXCLUDED.personal_fouls,
                    last_updated = CURRENT_TIMESTAMP
            """, values)
            self.conn.commit()

    def upsert_season_stats(self, season_stats, player_id, is_playoff=False):
        if season_stats.empty:
            return
            
        with self.conn.cursor() as cur:
            for _, row in season_stats.iterrows():
                cur.execute("""
                    INSERT INTO season_stats (
                        player_id, season, is_playoff, team, games_played,
                        points, rebounds, assists, steals, blocks,
                        minutes, field_goals_made, field_goals_attempted,
                        fg_percentage, three_points_made, three_points_attempted,
                        fg3_percentage, free_throws_made, free_throws_attempted,
                        ft_percentage, offensive_rebounds, defensive_rebounds,
                        turnovers, personal_fouls
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (player_id, season, is_playoff) DO UPDATE SET
                        team = EXCLUDED.team,
                        games_played = EXCLUDED.games_played,
                        points = EXCLUDED.points,
                        rebounds = EXCLUDED.rebounds,
                        assists = EXCLUDED.assists,
                        steals = EXCLUDED.steals,
                        blocks = EXCLUDED.blocks,
                        minutes = EXCLUDED.minutes,
                        field_goals_made = EXCLUDED.field_goals_made,
                        field_goals_attempted = EXCLUDED.field_goals_attempted,
                        fg_percentage = EXCLUDED.fg_percentage,
                        three_points_made = EXCLUDED.three_points_made,
                        three_points_attempted = EXCLUDED.three_points_attempted,
                        fg3_percentage = EXCLUDED.fg3_percentage,
                        free_throws_made = EXCLUDED.free_throws_made,
                        free_throws_attempted = EXCLUDED.free_throws_attempted,
                        ft_percentage = EXCLUDED.ft_percentage,
                        offensive_rebounds = EXCLUDED.offensive_rebounds,
                        defensive_rebounds = EXCLUDED.defensive_rebounds,
                        turnovers = EXCLUDED.turnovers,
                        personal_fouls = EXCLUDED.personal_fouls,
                        last_updated = CURRENT_TIMESTAMP
                """, (
                    player_id, row['SEASON_ID'], is_playoff, row.get('TEAM_ABBREVIATION', 'N/A'),
                    row['GP'], row['PTS'], row['REB'], row['AST'], row['STL'], row['BLK'],
                    row['MIN'], row['FGM'], row['FGA'],
                    float(str(row['FG_PCT']).rstrip('%')) / 100 if pd.notna(row['FG_PCT']) else None,
                    row['FG3M'], row['FG3A'],
                    float(str(row['FG3_PCT']).rstrip('%')) / 100 if pd.notna(row['FG3_PCT']) else None,
                    row['FTM'], row['FTA'],
                    float(str(row['FT_PCT']).rstrip('%')) / 100 if pd.notna(row['FT_PCT']) else None,
                    row['OREB'], row['DREB'], row['TOV'], row['PF']
                ))
            self.conn.commit()

    def upsert_season_averages(self, season_stats, player_id, is_playoff=False):
        if season_stats.empty:
            return
            
        with self.conn.cursor() as cur:
            for _, row in season_stats.iterrows():
                games = row['GP']
                if games > 0:
                    cur.execute("""
                        INSERT INTO season_averages (
                            player_id, season, is_playoff, games_played,
                            points_avg, rebounds_avg, assists_avg,
                            steals_avg, blocks_avg, minutes_avg
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (player_id, season, is_playoff) DO UPDATE SET
                            games_played = EXCLUDED.games_played,
                            points_avg = EXCLUDED.points_avg,
                            rebounds_avg = EXCLUDED.rebounds_avg,
                            assists_avg = EXCLUDED.assists_avg,
                            steals_avg = EXCLUDED.steals_avg,
                            blocks_avg = EXCLUDED.blocks_avg,
                            minutes_avg = EXCLUDED.minutes_avg,
                            last_updated = CURRENT_TIMESTAMP
                    """, (
                        player_id, row['SEASON_ID'], is_playoff, games,
                        row['PTS'] / games, row['REB'] / games,
                        row['AST'] / games, row['STL'] / games,
                        row['BLK'] / games, row['MIN'] / games
                    ))
            self.conn.commit()

    def upsert_awards(self, awards_list, player_id):
        if not awards_list:
            return
            
        with self.conn.cursor() as cur:
            for award in awards_list:
                count, name = award.split('x ', 1)
                cur.execute("""
                    INSERT INTO awards (player_id, award_name, count)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (player_id, award_name) DO UPDATE SET
                        count = EXCLUDED.count,
                        last_updated = CURRENT_TIMESTAMP
                """, (player_id, name.strip(), int(count)))
            self.conn.commit()

    def get_player_data(self, player_id):
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            # Get player basic info
            cur.execute("SELECT * FROM players WHERE player_id = %s", (player_id,))
            player_data = cur.fetchone()
            
            if not player_data:
                return None
                
            # Get all related data
            cur.execute("SELECT * FROM career_totals WHERE player_id = %s", (player_id,))
            career_totals = cur.fetchone()
            
            cur.execute("SELECT * FROM career_averages WHERE player_id = %s", (player_id,))
            career_averages = cur.fetchone()
            
            cur.execute("SELECT * FROM regular_season_totals WHERE player_id = %s ORDER BY season DESC", (player_id,))
            season_stats = cur.fetchall()
            
            cur.execute("SELECT * FROM playoff_totals WHERE player_id = %s ORDER BY season DESC", (player_id,))
            playoff_stats = cur.fetchall()
            
            cur.execute("SELECT * FROM regular_season_averages WHERE player_id = %s ORDER BY season DESC", (player_id,))
            season_averages = cur.fetchall()
            
            cur.execute("SELECT * FROM playoff_averages WHERE player_id = %s ORDER BY season DESC", (player_id,))
            playoff_averages = cur.fetchall()
            
            cur.execute("SELECT award_name, count FROM awards WHERE player_id = %s", (player_id,))
            awards = cur.fetchall()
            
            # Convert to the format expected by the application
            return {
                'player_details': dict(player_data),
                'career_totals': dict(career_totals) if career_totals else None,
                'career_averages': dict(career_averages) if career_averages else None,
                'season_stats': [dict(row) for row in season_stats],
                'playoff_stats': [dict(row) for row in playoff_stats],
                'season_averages': [dict(row) for row in season_averages],
                'playoff_averages': [dict(row) for row in playoff_averages],
                'awards': [f"{row['count']}x {row['award_name']}" for row in awards],
                'last_updated': player_data['last_updated']
            }

    def needs_update(self, stored_data):
        """Check if data is older than 24 hours"""
        if not stored_data or 'last_updated' not in stored_data:
            return True
        time_diff = datetime.now() - stored_data['last_updated']
        return time_diff.days >= 1

    def close(self):
        self.conn.close()

    def upsert_career_averages(self, career_totals, player_id):
        if career_totals.empty:
            return
        
        data = career_totals.iloc[0]
        games = int(data['GP'])
        
        values = (
            int(player_id),
            games,
            float(data['PTS']) / games,
            float(data['REB']) / games,
            float(data['AST']) / games,
            float(data['STL']) / games,
            float(data['BLK']) / games,
            float(data['MIN']) / games
        )
        
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO career_averages (
                    player_id, games_played, points_avg, rebounds_avg,
                    assists_avg, steals_avg, blocks_avg, minutes_avg
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id) DO UPDATE SET
                    games_played = EXCLUDED.games_played,
                    points_avg = EXCLUDED.points_avg,
                    rebounds_avg = EXCLUDED.rebounds_avg,
                    assists_avg = EXCLUDED.assists_avg,
                    steals_avg = EXCLUDED.steals_avg,
                    blocks_avg = EXCLUDED.blocks_avg,
                    minutes_avg = EXCLUDED.minutes_avg,
                    last_updated = CURRENT_TIMESTAMP
            """, values)
            self.conn.commit()

    def upsert_regular_season_totals(self, season_stats, player_id):
        if season_stats.empty:
            return
            
        for _, row in season_stats.iterrows():
            values = (
                int(player_id),
                row['SEASON_ID'],
                row.get('TEAM_ABBREVIATION', 'N/A'),
                int(row['GP']),
                float(row['PTS']),
                float(row['REB']),
                float(row['AST']),
                float(row['STL']),
                float(row['BLK']),
                float(row['MIN']),
                float(row['FGM']),
                float(row['FGA']),
                float(str(row['FG_PCT']).rstrip('%')) / 100 if pd.notna(row['FG_PCT']) else None,
                float(row['FG3M']),
                float(row['FG3A']),
                float(str(row['FG3_PCT']).rstrip('%')) / 100 if pd.notna(row['FG3_PCT']) else None,
                float(row['FTM']),
                float(row['FTA']),
                float(str(row['FT_PCT']).rstrip('%')) / 100 if pd.notna(row['FT_PCT']) else None,
                float(row['OREB']),
                float(row['DREB']),
                float(row['TOV']),
                float(row['PF'])
            )
            
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO regular_season_totals (
                        player_id, season, team, games_played, points, rebounds,
                        assists, steals, blocks, minutes, field_goals_made,
                        field_goals_attempted, fg_percentage, three_points_made,
                        three_points_attempted, fg3_percentage, free_throws_made,
                        free_throws_attempted, ft_percentage, offensive_rebounds,
                        defensive_rebounds, turnovers, personal_fouls
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (player_id, season) DO UPDATE SET
                        team = EXCLUDED.team,
                        games_played = EXCLUDED.games_played,
                        points = EXCLUDED.points,
                        rebounds = EXCLUDED.rebounds,
                        assists = EXCLUDED.assists,
                        steals = EXCLUDED.steals,
                        blocks = EXCLUDED.blocks,
                        minutes = EXCLUDED.minutes,
                        field_goals_made = EXCLUDED.field_goals_made,
                        field_goals_attempted = EXCLUDED.field_goals_attempted,
                        fg_percentage = EXCLUDED.fg_percentage,
                        three_points_made = EXCLUDED.three_points_made,
                        three_points_attempted = EXCLUDED.three_points_attempted,
                        fg3_percentage = EXCLUDED.fg3_percentage,
                        free_throws_made = EXCLUDED.free_throws_made,
                        free_throws_attempted = EXCLUDED.free_throws_attempted,
                        ft_percentage = EXCLUDED.ft_percentage,
                        offensive_rebounds = EXCLUDED.offensive_rebounds,
                        defensive_rebounds = EXCLUDED.defensive_rebounds,
                        turnovers = EXCLUDED.turnovers,
                        personal_fouls = EXCLUDED.personal_fouls,
                        last_updated = CURRENT_TIMESTAMP
                """, values)
                self.conn.commit()

    def upsert_playoff_totals(self, playoff_stats, player_id):
        if playoff_stats.empty:
            return
            
        for _, row in playoff_stats.iterrows():
            values = (
                int(player_id),
                row['SEASON_ID'],
                row.get('TEAM_ABBREVIATION', 'N/A'),
                int(row['GP']),
                float(row['PTS']),
                float(row['REB']),
                float(row['AST']),
                float(row['STL']),
                float(row['BLK']),
                float(row['MIN']),
                float(row['FGM']),
                float(row['FGA']),
                float(str(row['FG_PCT']).rstrip('%')) / 100 if pd.notna(row['FG_PCT']) else None,
                float(row['FG3M']),
                float(row['FG3A']),
                float(str(row['FG3_PCT']).rstrip('%')) / 100 if pd.notna(row['FG3_PCT']) else None,
                float(row['FTM']),
                float(row['FTA']),
                float(str(row['FT_PCT']).rstrip('%')) / 100 if pd.notna(row['FT_PCT']) else None,
                float(row['OREB']),
                float(row['DREB']),
                float(row['TOV']),
                float(row['PF'])
            )
            
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO playoff_totals (
                        player_id, season, team, games_played, points, rebounds,
                        assists, steals, blocks, minutes, field_goals_made,
                        field_goals_attempted, fg_percentage, three_points_made,
                        three_points_attempted, fg3_percentage, free_throws_made,
                        free_throws_attempted, ft_percentage, offensive_rebounds,
                        defensive_rebounds, turnovers, personal_fouls
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (player_id, season) DO UPDATE SET
                        team = EXCLUDED.team,
                        games_played = EXCLUDED.games_played,
                        points = EXCLUDED.points,
                        rebounds = EXCLUDED.rebounds,
                        assists = EXCLUDED.assists,
                        steals = EXCLUDED.steals,
                        blocks = EXCLUDED.blocks,
                        minutes = EXCLUDED.minutes,
                        field_goals_made = EXCLUDED.field_goals_made,
                        field_goals_attempted = EXCLUDED.field_goals_attempted,
                        fg_percentage = EXCLUDED.fg_percentage,
                        three_points_made = EXCLUDED.three_points_made,
                        three_points_attempted = EXCLUDED.three_points_attempted,
                        fg3_percentage = EXCLUDED.fg3_percentage,
                        free_throws_made = EXCLUDED.free_throws_made,
                        free_throws_attempted = EXCLUDED.free_throws_attempted,
                        ft_percentage = EXCLUDED.ft_percentage,
                        offensive_rebounds = EXCLUDED.offensive_rebounds,
                        defensive_rebounds = EXCLUDED.defensive_rebounds,
                        turnovers = EXCLUDED.turnovers,
                        personal_fouls = EXCLUDED.personal_fouls,
                        last_updated = CURRENT_TIMESTAMP
                """, values)
                self.conn.commit()

    def upsert_regular_season_averages(self, season_stats, player_id):
        if season_stats.empty:
            return
            
        for _, row in season_stats.iterrows():
            games = int(row['GP'])
            if games > 0:
                values = (
                    int(player_id),
                    row['SEASON_ID'],
                    row.get('TEAM_ABBREVIATION', 'N/A'),
                    games,
                    float(row['PTS']) / games,
                    float(row['REB']) / games,
                    float(row['AST']) / games,
                    float(row['STL']) / games,
                    float(row['BLK']) / games,
                    float(row['MIN']) / games
                )
                
                with self.conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO regular_season_averages (
                            player_id, season, team, games_played,
                            points_avg, rebounds_avg, assists_avg,
                            steals_avg, blocks_avg, minutes_avg
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (player_id, season) DO UPDATE SET
                            team = EXCLUDED.team,
                            games_played = EXCLUDED.games_played,
                            points_avg = EXCLUDED.points_avg,
                            rebounds_avg = EXCLUDED.rebounds_avg,
                            assists_avg = EXCLUDED.assists_avg,
                            steals_avg = EXCLUDED.steals_avg,
                            blocks_avg = EXCLUDED.blocks_avg,
                            minutes_avg = EXCLUDED.minutes_avg,
                            last_updated = CURRENT_TIMESTAMP
                    """, values)
                    self.conn.commit()

    def upsert_playoff_averages(self, playoff_stats, player_id):
        if playoff_stats.empty:
            return
            
        for _, row in playoff_stats.iterrows():
            games = int(row['GP'])
            if games > 0:
                values = (
                    int(player_id),
                    row['SEASON_ID'],
                    row.get('TEAM_ABBREVIATION', 'N/A'),
                    games,
                    float(row['PTS']) / games,
                    float(row['REB']) / games,
                    float(row['AST']) / games,
                    float(row['STL']) / games,
                    float(row['BLK']) / games,
                    float(row['MIN']) / games
                )
                
                with self.conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO playoff_averages (
                            player_id, season, team, games_played,
                            points_avg, rebounds_avg, assists_avg,
                            steals_avg, blocks_avg, minutes_avg
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (player_id, season) DO UPDATE SET
                            team = EXCLUDED.team,
                            games_played = EXCLUDED.games_played,
                            points_avg = EXCLUDED.points_avg,
                            rebounds_avg = EXCLUDED.rebounds_avg,
                            assists_avg = EXCLUDED.assists_avg,
                            steals_avg = EXCLUDED.steals_avg,
                            blocks_avg = EXCLUDED.blocks_avg,
                            minutes_avg = EXCLUDED.minutes_avg,
                            last_updated = CURRENT_TIMESTAMP
                    """, values)
                    self.conn.commit() 