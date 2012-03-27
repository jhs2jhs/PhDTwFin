import sqlite3
import sys

db_filename = "./result/twiter_finance.db"
conn = sqlite3.connect(db_filename)

db_filename_uk = "./result/tf_uk.db"
conn_uk = sqlite3.connect(db_filename_uk)
db_filename_world = "./result/tf_world.db"
conn_world = sqlite3.connect(db_filename_world)
db_filename_userdata = "./result/tf_userdata.db"
conn_userdata = sqlite3.connect(db_filename_userdata)
db_filename_checkin = "./result/tf_checkin.db"
conn_checkin = sqlite3.connect(db_filename_checkin)

sql_init = '''
CREATE TABLE IF NOT EXISTS sentiment (
  stm_id INTEGER PRIMARY KEY AUTOINCREMENT,
  stm TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS language (
  l_id INTEGER PRIMARY KEY AUTOINCREMENT,
  language TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS twitter_world (
  r_id INTEGER PRIMARY KEY AUTOINCREMENT,
  stm_id INTEGER NOT NULL,
  twitter_text TEXT,
  twitter_id INTEGER, 
  l_id INTEGER,
  FOREIGN KEY (stm_id) REFERENCES sentiment(stm_id),
  FOREIGN KEY (l_id) REFERENCES language(l_id)
);
CREATE TABLE IF NOT EXISTS twitter_uk (
  r_id INTEGER PRIMARY KEY AUTOINCREMENT,
  stm_id INTEGER NOT NULL,
  twitter_text TEXT,
  twitter_id INTEGER, 
  l_id INTEGER,
  FOREIGN KEY (stm_id) REFERENCES sentiment(stm_id),
  FOREIGN KEY (l_id) REFERENCES language(l_id)
);
CREATE TABLE IF NOT EXISTS twitter_world_simple (
  r_id INTEGER PRIMARY KEY AUTOINCREMENT,
  stm_id TEXT NOT NULL,
  twitter_text TEXT,
  twitter_id INTEGER, 
  l_id TEXT
);
CREATE TABLE IF NOT EXISTS twitter_uk_simple (
  r_id INTEGER PRIMARY KEY AUTOINCREMENT,
  stm_id TEXT NOT NULL,
  twitter_text TEXT,
  twitter_id INTEGER, 
  l_id TEXT
);
CREATE TABLE IF NOT EXISTS users_data (
  r_id INTEGER PRIMARY KEY AUTOINCREMENT,
  u_id INTEGER NOT NULL UNIQUE,
  status_count INTEGER,
  followers_count INTEGER,
  friends_count INTEGER
);
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
'''

def db_create():
    global conn, conn_uk, conn_world, conn_userdata, conn_checkin, sql_init
    c = conn.cursor()
    c.executescript(sql_init)
    conn.commit()
    c.close()
    ################ uk
    c = conn_uk.cursor()
    c.executescript(sql_init)
    conn.commit()
    c.close()
################
    c = conn_world.cursor()
    c.executescript(sql_init)
    conn.commit()
    c.close()
################
    c = conn_userdata.cursor()
    c.executescript(sql_init)
    conn.commit()
    c.close()
################
    c = conn_checkin.cursor()
    c.executescript(sql_init)
    conn.commit()
    c.close()

sql_insert_sentiment = '''
INSERT OR IGNORE INTO sentiment (stm) VALUES (?)
'''
sql_select_sentiment = '''
SELECT stm_id FROM sentiment WHERE stm = ?
'''
def db_in_sentiment(sentiment):
    global conn
    c = conn.cursor()
    c.execute(sql_insert_sentiment, (sentiment, ))
    conn.commit()
    c.execute(sql_select_sentiment, (sentiment, ))
    index = c.fetchone()[0]
    c.close()
    return index

sql_insert_language = '''
INSERT OR IGNORE INTO language (language) VALUES (?)
'''
sql_select_language = '''
SELECT l_id FROM language WHERE language = ?
'''
def db_in_language(language):
    global conn
    c = conn.cursor()
    c.execute(sql_insert_language, (language, ))
    conn.commit()
    c.execute(sql_select_language, (language, ))
    index = c.fetchone()[0]
    c.close()
    return index


sql_insert_words_world = '''
INSERT INTO twitter_world (stm_id, twitter_text, twitter_id, l_id) VALUES (?, ?, ?, ?)
'''
sql_insert_words_uk = '''
INSERT INTO twitter_uk (stm_id, twitter_text, twitter_id, l_id) VALUES (?, ?, ?, ?)
'''
def sentiment_txt_to_db(words, level):
    global conn
    c = conn.cursor()
    if len(words) == 4:
        sentiment = words[0]
        twitter_stm = words[1].decode('utf-8')
        twitter_id = words[2]
        language = words[3]
    else:
        raise Exception("input words should be 4 length")
    stm_id = db_in_sentiment(sentiment)
    l_id = db_in_language(language)
    if level == "world":
        sql = sql_insert_words_world
    if level == "uk":
        sql = sql_insert_words_uk
    c.execute(sql, (stm_id, twitter_stm, twitter_id, l_id, ))
    conn.commit()
    c.close()

sql_insert_words_users_data = '''
INSERT INTO users_data (u_id, status_count, followers_count, friends_count) VALUES (?, ?, ?, ?)
'''
def users_data_txt_to_db(words):
    global conn
    c = conn.cursor()
    if len(words) == 4:
        u_id = words[0]
        status_count = words[1]
        followers_count = words[2]
        friends_count = words[3]
    else:
        raise Exception("input words should be 4 length")
    sql = sql_insert_words_users_data
    c.execute(sql, (u_id, status_count, followers_count, friends_count, ))
    conn.commit()
    c.close()

sql_insert_words_checkin = '''
INSERT INTO checkin_data (u_id, tweet_id, latitude, longitude, createdat, text, place_id) VALUES (?, ?, ?, ?, ?, ?, ?)
'''
def checkin_txt_to_db(words):
    global conn
    c = conn.cursor()
    u_id = words[0]
    tweet_id = words[1]
    latitude = words[2]
    longitude = words[3]
    createdat = words[4]
    if len(words) == 7:
        text = unicode(words[5], encoding='UTF-8')
        place_id = unicode(words[6], encoding='UTF-8')
    if len(words) == 6:
        print 6, words
        sys.exist()
        raise Exception("input words should be 4 length")
    sql = sql_insert_words_checkin
    c.execute(sql, (u_id, tweet_id, latitude, longitude, createdat, text, place_id, ))
    #print words
    conn.commit()
    c.close()
'''
def checkin_txt_to_db_list(words_list):
    global conn
    c = conn.cursor()
    for words in words_list:
        u_id = words[0]
        tweet_id = words[1]
        latitude = words[2]
        longitude = words[3]
        createdat = words[4]
        if len(words) == 7:
            text = unicode(words[5], encoding='UTF-8')
            place_id = unicode(words[6], encoding='UTF-8')
        if len(words) == 6:
            print 6, words
            sys.exist()
            raise Exception("input words should be 4 length")
        sql = sql_insert_words_checkin
        c.execute(sql, (u_id, tweet_id, latitude, longitude, createdat, text, place_id, ))
    #print words    
    conn.commit()
    c.close()
'''



sql_insert_words_world_list = '''
INSERT INTO twitter_world_simple (stm_id, twitter_text, twitter_id, l_id) VALUES (?, ?, ?, ?)
'''
sql_insert_words_uk_list = '''
INSERT INTO twitter_uk_simple (stm_id, twitter_text, twitter_id, l_id) VALUES (?, ?, ?, ?)
'''
sql_insert_words_users_data_list = '''
INSERT INTO users_data (u_id, status_count, followers_count, friends_count) VALUES (?, ?, ?, ?)
'''
sql_insert_words_checkin_list = '''
INSERT INTO checkin_data (u_id, tweet_id, latitude, longitude, createdat, text, place_id) VALUES (?, ?, ?, ?, ?, ?, ?)
'''
def bulk_insert(words_list, conn, sql):
    #global conn
    c = conn.cursor()
    #sql = sql_insert_words_checkin
    #print sql
    #print "========"
    #print words_list
    #print words_list
    #for words in words_list:
    #    c.execute(sql, words)
    c.executemany(sql, words_list)
    conn.commit()
    c.close()


