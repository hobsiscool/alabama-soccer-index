CREATE TABLE IF NOT EXISTS games (
    game_id TEXT PRIMARY KEY,
    game_date TEXT,
    team TEXT,
    opponent TEXT,
    score_f INTEGER,
    score_a INTEGER,
    is_home BOOLEAN,
    is_neutral BOOLEAN,
    classification TEXT
);

CREATE INDEX IF NOT EXISTS idx_team ON games(team);

-- One-time cleanup in case any @ symbols snuck in
UPDATE games 
SET opponent = TRIM(REPLACE(opponent, '@', '')) 
WHERE opponent LIKE '@%';