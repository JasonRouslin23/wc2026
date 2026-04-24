-- ============================================================
-- WC2026 Database Schema
-- ============================================================

-- Groups A-L
CREATE TABLE IF NOT EXISTS groups (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(2) NOT NULL UNIQUE,  -- 'A' .. 'L'
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 48 National Teams
CREATE TABLE IF NOT EXISTS teams (
    id                  VARCHAR(64) PRIMARY KEY,   -- sr:competitor:XXXXX
    name                TEXT NOT NULL,
    short_name          TEXT,
    abbreviation        VARCHAR(8),
    country             TEXT,
    country_code        VARCHAR(8),
    group_name          VARCHAR(2),                -- FK-ish to groups.name
    gender              VARCHAR(8) DEFAULT 'male',
    -- Jersey colors (hex from SportRadar)
    jersey_primary      VARCHAR(7),
    jersey_secondary    VARCHAR(7),
    jersey_goalkeeper   VARCHAR(7),
    -- Manager
    manager_id          VARCHAR(64),
    manager_name        TEXT,
    manager_nationality TEXT,
    -- Power ranking fields (filled by model)
    power_rating        NUMERIC(6,3),
    power_rank          INT,
    tier                INT,
    -- Meta
    raw_json            JSONB,
    last_synced         TIMESTAMPTZ DEFAULT NOW()
);

-- Players / Squads
CREATE TABLE IF NOT EXISTS players (
    id                  VARCHAR(64) PRIMARY KEY,   -- sr:player:XXXXX
    team_id             VARCHAR(64) REFERENCES teams(id) ON DELETE CASCADE,
    name                TEXT NOT NULL,
    date_of_birth       DATE,
    nationality         TEXT,
    country_code        VARCHAR(8),
    position            VARCHAR(32),               -- Goalkeeper / Defender / Midfielder / Forward
    shirt_number        INT,
    height_cm           INT,
    weight_kg           INT,
    -- Model fields
    player_rating       NUMERIC(6,3),
    raw_json            JSONB,
    last_synced         TIMESTAMPTZ DEFAULT NOW()
);

-- Matches (friendlies, qualifiers, Nations League, WC group stage)
CREATE TABLE IF NOT EXISTS matches (
    id                  VARCHAR(64) PRIMARY KEY,   -- sr:sport_event:XXXXX
    season_id           VARCHAR(64),
    competition_id      VARCHAR(64),
    competition_name    TEXT,
    -- Teams
    home_team_id        VARCHAR(64) REFERENCES teams(id) ON DELETE SET NULL,
    away_team_id        VARCHAR(64) REFERENCES teams(id) ON DELETE SET NULL,
    -- Schedule
    kickoff_utc         TIMESTAMPTZ,
    status              VARCHAR(32),               -- not_started / live / closed / cancelled
    stage               TEXT,                      -- group_stage / qualifier / friendly / etc.
    group_name          VARCHAR(2),
    round               TEXT,
    -- Scores
    home_score          INT,
    away_score          INT,
    home_score_ht       INT,
    away_score_ht       INT,
    -- Venue
    venue_id            VARCHAR(64),
    venue_name          TEXT,
    venue_city          TEXT,
    venue_country       TEXT,
    -- Meta
    raw_json            JSONB,
    last_synced         TIMESTAMPTZ DEFAULT NOW()
);

-- Match lineups (one row per player per match)
CREATE TABLE IF NOT EXISTS match_lineups (
    id              SERIAL PRIMARY KEY,
    match_id        VARCHAR(64) REFERENCES matches(id) ON DELETE CASCADE,
    team_id         VARCHAR(64) REFERENCES teams(id) ON DELETE CASCADE,
    player_id       VARCHAR(64) REFERENCES players(id) ON DELETE CASCADE,
    starting_xi     BOOLEAN DEFAULT FALSE,
    position        VARCHAR(32),
    shirt_number    INT,
    UNIQUE(match_id, player_id)
);

-- Match events (goals, cards, subs)
CREATE TABLE IF NOT EXISTS match_events (
    id              SERIAL PRIMARY KEY,
    match_id        VARCHAR(64) REFERENCES matches(id) ON DELETE CASCADE,
    team_id         VARCHAR(64) REFERENCES teams(id) ON DELETE SET NULL,
    player_id       VARCHAR(64) REFERENCES players(id) ON DELETE SET NULL,
    event_type      VARCHAR(32),   -- goal / yellow_card / red_card / substitution
    minute          INT,
    extra_time      INT,
    method          TEXT,          -- penalty / own_goal / header / etc.
    raw_json        JSONB
);

-- Group standings (cached, refreshed after each matchday)
CREATE TABLE IF NOT EXISTS standings (
    id              SERIAL PRIMARY KEY,
    season_id       VARCHAR(64),
    group_name      VARCHAR(2),
    team_id         VARCHAR(64) REFERENCES teams(id) ON DELETE CASCADE,
    rank            INT,
    played          INT DEFAULT 0,
    wins            INT DEFAULT 0,
    draws           INT DEFAULT 0,
    losses          INT DEFAULT 0,
    goals_for       INT DEFAULT 0,
    goals_against   INT DEFAULT 0,
    goal_diff       INT GENERATED ALWAYS AS (goals_for - goals_against) STORED,
    points          INT DEFAULT 0,
    raw_json        JSONB,
    last_synced     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(season_id, group_name, team_id)
);

-- Sync log — track what was last fetched and when
CREATE TABLE IF NOT EXISTS sync_log (
    id          SERIAL PRIMARY KEY,
    job         TEXT NOT NULL,          -- 'teams', 'squads', 'form', 'standings'
    status      VARCHAR(16),            -- 'ok' / 'error'
    records     INT,
    message     TEXT,
    ran_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_teams_group ON teams(group_name);
CREATE INDEX IF NOT EXISTS idx_players_team ON players(team_id);
CREATE INDEX IF NOT EXISTS idx_matches_home ON matches(home_team_id);
CREATE INDEX IF NOT EXISTS idx_matches_away ON matches(away_team_id);
CREATE INDEX IF NOT EXISTS idx_matches_kickoff ON matches(kickoff_utc DESC);
CREATE INDEX IF NOT EXISTS idx_standings_group ON standings(group_name);
CREATE INDEX IF NOT EXISTS idx_match_lineups_match ON match_lineups(match_id);
CREATE INDEX IF NOT EXISTS idx_match_events_match ON match_events(match_id);
