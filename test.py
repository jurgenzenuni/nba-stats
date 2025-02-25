from nba_api.stats.endpoints import playercareerstats, playerawards, commonplayerinfo
import pandas as pd

jokic_career= playercareerstats.PlayerCareerStats(203999)

df = jokic_career.career_totals_regular_season.get_data_frame()
df2 = jokic_career.career_totals_post_season.get_data_frame()
df3 = jokic_career.season_totals_regular_season.get_data_frame()
df4 = jokic_career.season_totals_post_season.get_data_frame()

# print(df3)


# Get player awards
embiid_awards = playerawards.PlayerAwards(203954)
awards_df = embiid_awards.get_data_frames()[0]
# Group awards by type and count
award_counts = awards_df['DESCRIPTION'].value_counts().sort_values(ascending=False)  # sort by count

for award, count in award_counts.items():
    print(f"{count}x {award}")


# Get player info 
player_info = commonplayerinfo.CommonPlayerInfo(203954)
df5 = player_info.get_data_frames()[0]
print(df5)
