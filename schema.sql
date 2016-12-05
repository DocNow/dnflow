DROP TABLE IF EXISTS searches;
CREATE TABLE searches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    text TEXT NOT NULL,
    date_path TEXT NOT NULL,
    user TEXT NOT NULL,
    status TEXT,
    created DATETIME DEFAULT CURRENT_TIMESTAMP,
    published DATETIME
);

DROP TABLE IF EXISTS trend_locations;
CREATE TABLE trend_locations (
    woeid INTEGER PRIMARY KEY,
    country_code CHAR(2),
    url TEXT,
    name TEXT NOT NULL,
    country TEXT,
    type_name TEXT,
    type_code INTEGER,
    parent_woeid INTEGER
);

DROP TABLE IF EXISTS trends;
CREATE TABLE trends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    woeid INTEGER NOT NULL,
    ts_fetch DATETIME NOT NULL,
    query TEXT,
    tweet_volume INTEGER DEFAULT 0,
    url TEXT,
    promoted_content BOOLEAN DEFAULT 0,
    name TEXT NOT NULL
);
