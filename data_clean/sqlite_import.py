##### The final database shoule be like this 
CREATE TABLE IF NOT EXISTS checkin_data (
  r_id INTEGER PRIMARY KEY AUTOINCREMENT,
  u_id INTEGER,
  tweet_id REAL, 
  latitude REAL, 
  longitude REAL, 
  createdat TEXT,
  text TEXT, 
  place_id TEXT
);
CREATE TABLE IF NOT EXISTS users_data (
  r_id INTEGER PRIMARY KEY AUTOINCREMENT,
  u_id INTEGER NOT NULL UNIQUE,
  status_count INTEGER,
  followers_count INTEGER,
  friends_count INTEGER
);
CREATE TABLE IF NOT EXISTS sentiment_world (
  r_id INTEGER PRIMARY KEY AUTOINCREMENT,
  stm TEXT NOT NULL,
  twitter_text TEXT,
  twitter_id INTEGER, 
  language TEXT,
);
CREATE TABLE IF NOT EXISTS sentiment_uk (
  r_id INTEGER PRIMARY KEY AUTOINCREMENT,
  stm TEXT NOT NULL,
  twitter_text TEXT,
  twitter_id INTEGER, 
  language TEXT,
);


########### sqlite import temp table should 
CREATE TABLE IF NOT EXISTS checkin_data (
  u_id INTEGER,
  tweet_id REAL, 
  latitude REAL, 
  longitude REAL, 
  createdat TEXT,
  empty TEXT
);
CREATE TABLE IF NOT EXISTS users_data (
  u_id INTEGER NOT NULL UNIQUE,
  status_count INTEGER,
  followers_count INTEGER,
  friends_count INTEGER
);
CREATE TABLE IF NOT EXISTS sentiment_world (
  stm TEXT NOT NULL,
  twitter_id INTEGER, 
  emtpy TEXT
);
CREATE TABLE IF NOT EXISTS sentiment_uk (
  stm TEXT NOT NULL,
  twitter_id INTEGER, 
  empty TEXT
);



#### sqlite3 command
CREATE TALE checkin_data ....
.separator \t
.import "file" checkin_data
