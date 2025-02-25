-- Players basic info
CREATE TABLE players (
    player_id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    position VARCHAR(50),
    height VARCHAR(20),
    weight VARCHAR(20),
    birth_date DATE,
    country VARCHAR(100),
    experience INTEGER,
    jersey_number VARCHAR(10),
    team VARCHAR(100),
    image_url VARCHAR(255),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Career totals (regular season)
CREATE TABLE career_totals (
    player_id INTEGER PRIMARY KEY,
    games_played INTEGER,
    points DECIMAL(10,1),
    rebounds DECIMAL(10,1),
    assists DECIMAL(10,1),
    steals DECIMAL(10,1),
    blocks DECIMAL(10,1),
    minutes DECIMAL(10,1),
    field_goals_made DECIMAL(10,1),
    field_goals_attempted DECIMAL(10,1),
    fg_percentage DECIMAL(5,1),
    three_points_made DECIMAL(10,1),
    three_points_attempted DECIMAL(10,1),
    fg3_percentage DECIMAL(5,1),
    free_throws_made DECIMAL(10,1),
    free_throws_attempted DECIMAL(10,1),
    ft_percentage DECIMAL(5,1),
    offensive_rebounds DECIMAL(10,1),
    defensive_rebounds DECIMAL(10,1),
    turnovers DECIMAL(10,1),
    personal_fouls DECIMAL(10,1),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE
);

-- Career averages
CREATE TABLE career_averages (
    player_id INTEGER PRIMARY KEY,
    games_played INTEGER,
    points_avg DECIMAL(5,1),
    rebounds_avg DECIMAL(5,1),
    assists_avg DECIMAL(5,1),
    steals_avg DECIMAL(5,1),
    blocks_avg DECIMAL(5,1),
    minutes_avg DECIMAL(5,1),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE
);

-- Regular season totals by season
CREATE TABLE regular_season_totals (
    player_id INTEGER,
    season VARCHAR(10),
    team VARCHAR(100),
    games_played INTEGER,
    points DECIMAL(10,1),
    rebounds DECIMAL(10,1),
    assists DECIMAL(10,1),
    steals DECIMAL(10,1),
    blocks DECIMAL(10,1),
    minutes DECIMAL(10,1),
    field_goals_made DECIMAL(10,1),
    field_goals_attempted DECIMAL(10,1),
    fg_percentage DECIMAL(5,1),
    three_points_made DECIMAL(10,1),
    three_points_attempted DECIMAL(10,1),
    fg3_percentage DECIMAL(5,1),
    free_throws_made DECIMAL(10,1),
    free_throws_attempted DECIMAL(10,1),
    ft_percentage DECIMAL(5,1),
    offensive_rebounds DECIMAL(10,1),
    defensive_rebounds DECIMAL(10,1),
    turnovers DECIMAL(10,1),
    personal_fouls DECIMAL(10,1),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (player_id, season),
    FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE
);

-- Regular season averages by season
CREATE TABLE regular_season_averages (
    player_id INTEGER,
    season VARCHAR(10),
    team VARCHAR(100),
    games_played INTEGER,
    points_avg DECIMAL(5,1),
    rebounds_avg DECIMAL(5,1),
    assists_avg DECIMAL(5,1),
    steals_avg DECIMAL(5,1),
    blocks_avg DECIMAL(5,1),
    minutes_avg DECIMAL(5,1),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (player_id, season),
    FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE
);

-- Playoff totals by season
CREATE TABLE playoff_totals (
    player_id INTEGER,
    season VARCHAR(10),
    team VARCHAR(100),
    games_played INTEGER,
    points DECIMAL(10,1),
    rebounds DECIMAL(10,1),
    assists DECIMAL(10,1),
    steals DECIMAL(10,1),
    blocks DECIMAL(10,1),
    minutes DECIMAL(10,1),
    field_goals_made DECIMAL(10,1),
    field_goals_attempted DECIMAL(10,1),
    fg_percentage DECIMAL(5,1),
    three_points_made DECIMAL(10,1),
    three_points_attempted DECIMAL(10,1),
    fg3_percentage DECIMAL(5,1),
    free_throws_made DECIMAL(10,1),
    free_throws_attempted DECIMAL(10,1),
    ft_percentage DECIMAL(5,1),
    offensive_rebounds DECIMAL(10,1),
    defensive_rebounds DECIMAL(10,1),
    turnovers DECIMAL(10,1),
    personal_fouls DECIMAL(10,1),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (player_id, season),
    FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE
);

-- Playoff averages by season
CREATE TABLE playoff_averages (
    player_id INTEGER,
    season VARCHAR(10),
    team VARCHAR(100),
    games_played INTEGER,
    points_avg DECIMAL(5,1),
    rebounds_avg DECIMAL(5,1),
    assists_avg DECIMAL(5,1),
    steals_avg DECIMAL(5,1),
    blocks_avg DECIMAL(5,1),
    minutes_avg DECIMAL(5,1),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (player_id, season),
    FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE
);

-- Awards
CREATE TABLE awards (
    player_id INTEGER,
    award_name VARCHAR(200),
    count INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (player_id, award_name),
    FOREIGN KEY (player_id) REFERENCES players(player_id) ON DELETE CASCADE
);

-- Indexes for better query performance
CREATE INDEX idx_players_name ON players(name);
CREATE INDEX idx_regular_season_totals_season ON regular_season_totals(season);
CREATE INDEX idx_playoff_totals_season ON playoff_totals(season);
CREATE INDEX idx_regular_season_averages_season ON regular_season_averages(season);
CREATE INDEX idx_playoff_averages_season ON playoff_averages(season);
CREATE INDEX idx_awards_name ON awards(award_name);
CREATE INDEX idx_players_last_updated ON players(last_updated); 