import urllib2
from BeautifulSoup import BeautifulSoup
import sqlite3
import json
import codecs
import time

db_path = './tweets/tweet.db'
conn = sqlite3.connect(db_path)

web_openner = urllib2.build_opener(urllib2.HTTPHandler)
twidder_url = 'http://id.twidder.info/cgi-bin'

def twitter_screenname_to_id(screenname):
    url = '%s/tw_id?UserName=%s'%(twidder_url, str(screenname))
    html = web_openner.open(url).read()
    value = twidder_get_value(html, {'name':'UserID'})
    return value
    
def twitter_id_to_screenname(id):
    url = '%s/tw_un?UserID=%s'%(twidder_url, str(id))
    html = web_openner.open(url).read()
    value = twidder_get_value(html, {'name':'UserName'})       
    return value

def twidder_get_value(html, attrs_dict):
    try:
        soup = BeautifulSoup(html)
        l_value = soup.findAll('input', attrs=attrs_dict)
        value = l_value[0]['value']
        print value
    except Exception, e:
        print e
        value = ''
    return value

def twitter_id_match(file_source):
    f = open(file_source, 'r')
    fo = open(file_source+".output.txt", 'w')
    fo.write('tw_id \ttw_name \tname \tcomments\n')
    while 1:
        lines = f.readlines(1)
        if not lines:
            break
        #print len(line)
        for l in lines:
            l = l.strip('\r\n')
            ls = l.split('\t')
            print ls
            t_name = ls[0].strip(':')
            t_comment = ls[2].strip()
            t_id = ls[1].strip()
            if t_id != 'N/A':
                t_screenname = twitter_id_to_screenname(t_id)
            else:
                t_screenname = 'N/A'
            fo.write('%s\t%s\t%s\t%s\n'%(t_id, t_screenname, t_name, t_comment))
    f.close()
    fo.close()

tw_type_analyst = 'ANALYST'
tw_type_organisation = 'ORGANISATION'

sql_init = '''
CREATE TABLE IF NOT EXISTS tw (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tw_id TEXT UNIQUE NOT NULL, 
  tw_name TEXT,
  full_name TEXT,
  tw_type TEXT,
  create_time TIMESTAMP DEFAULT (DATETIME('now'))
);
CREATE TABLE IF NOT EXISTS tw_get (
  tw_id TEXT UNIQUE NOT NULL,
  max_id TEXT,
  max_time TIMESTAMP DEFAULT (DATETIME('now')),
  min_id TEXT,
  min_time TIMESTAMP DEFAULT (DATETIME('now')),
  min_finish INTEGER NOT NULL DEFAULT 0,
  FOREIGN KEY (tw_id) REFERENCES tw(tw_id)
);
CREATE TABLE IF NOT EXISTS tw_user (
  tw_id TEXT UNIQUE NOT NULL,
  tw_name TEXT,
  tw_screenname TEXT,
  tw_location TEXT,
  tw_desc TEXT,
  tw_url TEXT,
  tw_followers TEXT, --count
  tw_friends TEXT,  -- count
  tw_listed TEXT,  -- count
  tw_created_at TEXT,  -- time
  tw_status TEXT,  -- count
  create_time TIMESTAMP DEFAULT (DATETIME('now')),
  FOREIGN KEY (tw_id) REFERENCES tw(tw_id)
);
CREATE TABLE IF NOT EXISTS tw_status (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tid TEXT UNIQUE NOT NULL, 
  tw_text TEXT, 
  source TEXT, 
  reply_to_status TEXT, 
  reply_to_user TEXT, 
  reply_to_username TEXT, 
  geo TEXT, 
  coordinates TEXT, 
  place TEXT,
  contributors TEXT,
  retweet_count TEXT, 
  favorited TEXT,
  retweeted TEXT,
  possibly_sensitive TEXT, 
  retweet_status_id TEXT,
----
  hashtags TEXT,
  urls TEXT,
  user_mentions TEXT,
----
  tw_id TEXT, 
  tw_create_at TEXT, 
  create_time TIMESTAMP DEFAULT (DATETIME('now')),
  FOREIGN KEY (tw_id) REFERENCES tw(tw_id)
);
-----------
CREATE TABLE IF NOT EXISTS klout_user (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  klout_id TEXT UNIQUE NOT NULL,
  klout_name TEXT,
  klout_score TEXT
);
CREATE TABLE IF NOT EXISTS klout_twitter (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tw_id TEXT UNIQUE NOT NULL,
  klout_id TEXT DEFAULT 0, 
  code TEXT DEFAULT 0,
  FOREIGN KEY (tw_id) REFERENCES tw(tw_id)
  ----FOREIGN KEY (klout_id) REFERENCES klout(klout_id)
);
CREATE TABLE IF NOT EXISTS klout_score (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  klout_id TEXT, 
  klout_score TEXT, 
  klout_scoredelta TEXT, 
  create_date TEXT DEFAULT (date('now')) UNIQUE NOT NULL,
  FOREIGN KEY (klout_id) REFERENCES klout_user(klout_id)
);
CREATE TABLE IF NOT EXISTS klout_influence (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  klout_id TEXT, 
  influence_klout_id TEXT, 
  create_date TEXT DEFAULT (date('now')) NOT NULL,
  type TEXT,
  FOREIGN KEY (klout_id) REFERENCES klout_user(klout_id),
  FOREIGN KEY (influence_klout_id) REFERENCES klout_user(klout_id),
  UNIQUE (klout_id, influence_klout_id, create_date, type)
);
CREATE TABLE IF NOT EXISTS klout_influence_count (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  klout_id TEXT UNIQUE NOT NULL,
  influencer_count TEXT DEFAULT 0,
  influencee_count TEXT DEFAULT 0,
  FOREIGN KEY (klout_id) REFERENCES klout_user(klout_id)
);
CREATE TABLE IF NOT EXISTS klout_topic (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic_id TEXT UNIQUE NOT NULL, 
  topic_displayname TEXT, 
  topic_name TEXT
);
CREATE TABLE IF NOT EXISTS klout_topic_user (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  topic_id TEXT, 
  klout_id TEXT, 
  FOREIGN KEY (topic_id) REFERENCES klout_topic(topic_id),
  FOREIGN KEY (klout_id) REFERENCES klout_user(klout_id),
  UNIQUE (topic_id, klout_id)
);
CREATE TABLE IF NOT EXISTS trstrank (
  tw_id TEXT UNIQUE NOT NULL,
  tq TEXT, 
  trstrank TEXT, 
  code TEXT DEFAULT 0
);
CREATE TABLE IF NOT EXISTS influence_metrics (
  tw_id TEXT UNIQUE NOT NULL, 
  at_trstrank TEXT, 
  chattness TEXT, 
  enthusiasm TEXT, 
  feedness TEXT, 
  fo_trstrank TEXT, 
  follow_rate TEXT, 
  influx TEXT, 
  interesting TEXT, 
  outflux TEXT, 
  sway TEXT, 
  code TEXT DEFAULT 0
);
'''

def db_init():
    c = conn.cursor()
    c.executescript(sql_init)
    conn.commit()
    c.execute('SELECT * FROM SQLITE_MASTER')
    tables = c.fetchall()
    print '** tables total number: '+str(len(tables))
    c.close()

sql_tw_insert = '''
INSERT OR IGNORE INTO tw (tw_id, tw_name, full_name, tw_type) VALUES (?, ?, ?, ?)
'''
sql_user_insert = '''
INSERT OR IGNORE INTO tw_user (tw_id) VALUES (?)
'''
sql_get_insert = '''
INSERT OR IGNORE INTO tw_get (tw_id) VALUES (?)
'''
def tw_init(file_source, tw_type):
    f = open(file_source, 'r')
    c = conn.cursor()
    i = 0
    while 1:
        line = f.readline()
        i = i + 1
        if not line:
            break
        if i <= 2:
            continue
        line = line.strip()
        ls = line.split('\t')
        tw_id = ls[0]
        tw_name = ls[1]
        full_name = ls[2]
        c.execute(sql_tw_insert, (tw_id, tw_name, full_name, tw_type))
        c.execute(sql_user_insert, (tw_id, ))
        c.execute(sql_get_insert, (tw_id, ))
        conn.commit()
        #print ls
    f.close()
    c.close()

sql_user_update = '''
UPDATE tw_user SET tw_name = ?, tw_screenname = ?, tw_location = ?, tw_desc = ?, tw_url = ?, tw_followers = ?, tw_friends = ?, tw_listed = ?, tw_created_at = ?, tw_status = ? WHERE tw_id = ?
'''
def db_user_update(tw_name, tw_screenname, tw_location, tw_desc, tw_url, tw_followers, tw_friends, tw_listed, tw_created_at, tw_status, tw_id):
    c = conn.cursor()
    c.execute(sql_user_update, (tw_name, tw_screenname, tw_location, tw_desc, tw_url, tw_followers, tw_friends, tw_listed, tw_created_at, tw_status, tw_id))
    conn.commit()
    c.close()

sql_get_update_max = '''
UPDATE tw_get SET max_id = ?, max_time = (DATETIME('now')) WHERE tw_id = ?
'''
sql_get_update_min = '''
UPDATE tw_get SET min_id = ?, min_time = (DATETIME('now')) WHERE tw_id = ?
'''
sql_get_update_min_finish = '''
UPDATE tw_get SET min_finish = ? WHERE tw_id = ?
'''
def db_get_update(type, tw_id, id):
    c = conn.cursor()
    if type == 'max':
        c.execute(sql_get_update_max, (id, tw_id))
    if type == 'min':
        c.execute(sql_get_update_min, (id, tw_id))
    if type == 'min_finish':
        #print "==min_finish:", id, tw_id
        c.execute(sql_get_update_min_finish, (id, tw_id))
    conn.commit()
    c.close()

sql_status_insert = '''
INSERT OR IGNORE INTO tw_status (tid, tw_text, tw_create_at, source, reply_to_status, reply_to_user, reply_to_username, geo, coordinates, place, contributors, retweet_count, favorited, retweeted, possibly_sensitive, retweet_status_id, hashtags, urls, user_mentions, tw_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''
def db_status_insert(tid, tw_text, tw_create_at, source, reply_to_status, reply_to_user, reply_to_username, geo, coordinates, place, contributors, retweet_count, favorited, retweeted, possibly_sensitive, retweet_status_id, hashtags, urls, user_mentions, tw_id):
    c = conn.cursor()
    #print tid, tw_text, source, reply_to_status, reply_to_user, reply_to_username, geo, coordinates, place, contributors, retweet_count, favorited, retweeted, possibly_sensitive, retweet_status_id, hashtags, urls, user_mentions, tw_id
    c.execute(sql_status_insert, (tid, tw_text, tw_create_at, source, reply_to_status, reply_to_user, reply_to_username, geo, coordinates, place, contributors, retweet_count, favorited, retweeted, possibly_sensitive, retweet_status_id, hashtags, urls, user_mentions, tw_id))
    conn.commit()
    c.close()


def twitter_response_parse(html, tw_id):
    js = json.loads(html)
    max_id = 0
    min_id = 0
    #tw_id = 0
    i = 0
    for j in js:
        tw_created_at = j['created_at']
        tid = j['id']
        tw_text = j['text'] ##
        source = j['source']
        reply_to_status = j['in_reply_to_status_id']
        reply_to_user = j['in_reply_to_user_id']
        reply_to_username = j['in_reply_to_screen_name']
        geo = j['geo']
        geo = json.dumps(geo)
        coordinates = j['coordinates']
        coordinates = json.dumps(coordinates)
        place = j['place']
        place = json.dumps(place)
        contributors = j['contributors']
        retweet_count = j['retweet_count']
        favorited = j['favorited']
        retweeted = j['retweeted']
        if j.has_key('possibly_sensitive'):
            possibly_sensitive = j['possibly_sensitive']
        else:
            possibly_sensitive = ''
        # may have retweeted_status
        if j.has_key('retweeted_status'):
            retweet_status_id = j['retweeted_status']['id']
        else:
            retweet_status_id = ''
        # entity
        hashtags = j['entities']['hashtags']
        hashtags = json.dumps(hashtags)
        urls = j['entities']['urls']
        urls = json.dumps(urls)
        user_mentions = j['entities']['user_mentions']
        user_mentions = json.dumps(user_mentions)
        # user
        user = j['user']
        #tw_id = user['id']
        tw_name= user['name']
        tw_screenname = user['screen_name']
        tw_location = user['location']
        tw_desc = user['description']
        tw_url = user['url']
        tw_followers = user['followers_count']
        tw_friends = user['friends_count']
        tw_listed = user['listed_count']
        tw_created_at = user['created_at']
        tw_status = user['statuses_count']
        #
        db_status_insert(tid, tw_text, tw_created_at, source, reply_to_status, reply_to_user, reply_to_username, geo, coordinates, place, contributors, retweet_count, favorited, retweeted, possibly_sensitive, retweet_status_id, hashtags, urls, user_mentions, tw_id)
        db_user_update(tw_name, tw_screenname, tw_location, tw_desc, tw_url, tw_followers, tw_friends, tw_listed, tw_created_at, tw_status, tw_id)
        #
        if i == 0:
            max_id = tid
            min_id = tid
            i = i + 1
        if max_id < tid:
            max_id = tid
        if min_id > tid:
            min_id = tid
        #print created_at, tw_name
        #tw_id = tid
    print 'twitter_response_parse', tw_id
    if tw_id != 0:
        tw_get_id_update(tw_id, max_id, min_id)
        tw_get_min_finish_check(tw_id)
    print "tweet len:", len(js)
    if len(js) < 199:
        db_get_update('min_finish', tw_id, 1)

sql_user_status_get = '''
SELECT tw_status FROM tw_user WHERE tw_id = ?
'''
sql_status_user_count = '''
SELECT COUNT(*) FROM tw_status WHERE tw_id = ?
'''
def tw_get_min_finish_check(tw_id):
    c = conn.cursor()
    c.execute(sql_user_status_get, (tw_id, ))
    raw = c.fetchone()
    status_user = raw[0] # how many he should posted
    if status_user == None:
        status_user = '0'
    c.execute(sql_status_user_count, (tw_id, ))
    raw = c.fetchone()
    status_count = raw[0] # how many we download
    if status_count == None:
        status_count = '0'
    if int(status_user) <= int(status_count):
        db_get_update('min_finish', tw_id, 1)
    c.close()
    
sql_get_get = '''
SELECT * FROM tw_get WHERE tw_id = ?
'''
def tw_get_id_update(tw_id, max_id, min_id):
    c = conn.cursor()
    c.execute(sql_get_get, (tw_id, ))
    raw = c.fetchone()
    #print tw_id, raw
    max_id_db = raw[1]
    min_id_db = raw[3]
    #max_id_db = int(max_id_db)
    #max_id = int(max_id)
    #min_id_db = int(min_id_db)
    #min_id = int(min_id)
    #print max_id_db, min_id_db, max_id, min_id
    if max_id_db == None or int(max_id_db) < int(max_id):
        db_get_update('max', tw_id, max_id)
    if min_id_db == None or int(min_id_db) > int(min_id):
        db_get_update('min', tw_id, min_id)
    c.close()


           
def twitter_api_read(url, tw_id, loop_type):
    print "========"+tw_id+"========"
    try:
        read = web_openner.open(url)
        html = read.read()
    # ready to parse error 
        twitter_response_parse(html, tw_id)
        if loop_type == 'min': ######### if only cover min at moment
            twitter_read_loop_min(tw_id)
    except urllib2.HTTPError, e:
        print "HttpError:"+tw_id, 
        print e.code, e
        if e.code == 401 or e.code == '401':
            db_get_update('min_finish', tw_id, 401)
    except urllib2.URLError, e:
        print "URLError:"+tw_id
        print e.reason, e
    #except Exception, e:
    #    print "****Error with %s"%tw_id
    #    print e

sql_get_min_finish_get = '''
SELECT min_finish FROM tw_get WHERE tw_id = ?
'''
sql_get_min_get = '''
SELECT min_id FROM tw_get WHERE tw_id = ?
'''
def twitter_read_loop_min(tw_id):
    c = conn.cursor()
    c.execute(sql_get_min_finish_get, (tw_id, ))
    raw = c.fetchone()
    min_finish = raw[0]
    print 'twitter_read_loop_min min_finish:', min_finish
    if min_finish == 0:
        c.execute(sql_get_min_get, (tw_id, ))
        raw = c.fetchone()
        min_id = raw[0]
        url = 'https://api.twitter.com/1/statuses/user_timeline.json?include_entities=true&include_rts=true&user_id=%s&count=199&max_id=%s'%(tw_id, min_id)
        twitter_api_read(url, tw_id, 'min')
    c.close()

def twitter_read(tw_id):
    c = conn.cursor()
    c.execute(sql_get_min_get, (tw_id, ))
    raw = c.fetchone()
    min_id = raw[0]
    ########### better to check whether it should loop from max or min
    #print "twitter_read:", tw_id, min_id, type(min_id)
    #print min_id, type(min_id)
    if min_id == None:
        url = 'https://api.twitter.com/1/statuses/user_timeline.json?include_entities=true&include_rts=true&user_id=%s&count=199'%tw_id
        twitter_api_read(url, tw_id, 'min')
    else:
        twitter_read_loop_min(tw_id)

sql_tw_user_get = '''
SELECT * FROM tw_get WHERE min_finish = 0
'''
def twitter_api_read_main():
    c = conn.cursor()
    c.execute(sql_tw_user_get, ())
    i = 0
    for raw in c.fetchall():
        #if i < 5:
        #    continue
        tw_id = raw[0]
        twitter_read(tw_id)
        #if i == 3:
            #break
        i = i+1
        print 'i:', i
    c.close()

sql_status_get_all = '''
SELECT * FROM tw_status
'''
def all_status_to_txt():
    f = codecs.open('./tweets/twitter_status_all.txt', mode='w', encoding='utf-8')
    f.write('twitter_id \ttweet \ttweet_id \ttweet_create_at \tretweet_count \tfavorited_count \tretweet \thashtags \turls \tuser_mentions \treplay_to_status \treply_to_user \tgeo \tcoordinates \tplace\n')
    c = conn.cursor()
    c.execute(sql_status_get_all, ())
    for raw in c.fetchall():
        tid = raw[1]
        tw_text = raw[2].strip().replace('\n', '').replace('\r', '')
        reply_to_status = raw[4]
        reply_to_user = raw[5]
        geo = raw[7]
        coordinates = raw[8]
        place = raw[9]
        retweet_count = raw[11]
        favorited = raw[12]
        retweet = raw[13]
        hashtags = raw[16]
        urls = raw[17]
        user_mentions = raw[18]
        tw_id = raw[19]
        tw_create_at = raw[20]
        print tw_id, tw_text, tid, tw_create_at, retweet_count, favorited, retweet, hashtags, urls, user_mentions, reply_to_status, reply_to_status, reply_to_user, geo, coordinates, place
        f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n'%(tw_id, tw_text, tid, tw_create_at, retweet_count, favorited, retweet, hashtags, urls, user_mentions, reply_to_status, reply_to_user, geo, coordinates, place))
    c.close()
    f.close()


sql_klout_twitter_insert = '''
INSERT OR IGNORE INTO klout_twitter (tw_id) VALUES (?)
'''
sql_klout_twitter_code_get = '''
SELECT code FROM klout_twitter WHERE tw_id = ?
'''
sql_user_all_get = '''
SELECT * FROM tw_user
'''
def klout_user_list():
    c = conn.cursor()
    c.execute(sql_user_all_get, ())
    for raw in c.fetchall():
        tw_id = raw[0]
        tw_name = raw[1]
        ##
        kwc = conn.cursor()
        kwc.execute(sql_klout_twitter_insert, (tw_id, ))
        conn.commit()
        kwc.close()
        ##
        ccode = conn.cursor()
        ccode.execute(sql_klout_twitter_code_get, (tw_id, ))
        code = ccode.fetchone()[0]
        if code == 0 or code == '0':
            print "==TW:", tw_id, tw_name
            kloutid = get_kloutid(tw_id)
            time.sleep(1)
        ccode.close()
        ##
    c.close()

sql_klout_user_insert = '''
INSERT OR IGNORE INTO klout_user (klout_id, klout_name, klout_score) VALUES (?, ?, ?)
'''
sql_klout_influence_count_insert = '''
INSERT OR IGNORE INTO klout_influence_count (klout_id) VALUES (?)
'''
sql_klout_twitter_user_update = '''
UPDATE klout_twitter SET klout_id = ? WHERE tw_id = ?
'''
sql_klout_twitter_code_update = '''
UPDATE klout_twitter SET code = ? WHERE tw_id = ?
'''
def get_kloutid(tw_id):
    url = 'http://api.klout.com/v2/identity.json/tw/%s?key=u9cvz5rjmrmskzm6gmsnk9ps'%tw_id
    #print url
    try:
        web = web_openner.open(url).read()
        js = json.loads(web)
        if js.has_key('id'):
            kloutid = js['id']
            ###
            c = conn.cursor()
            c.execute(sql_klout_user_insert, (kloutid, '', ''))
            c.execute(sql_klout_influence_count_insert, (kloutid, ))
            c.execute(sql_klout_twitter_user_update, (kloutid, tw_id))
            conn.commit()
            c.close()
            ###
            #kloutid = '635263'
            print "kloutid:", kloutid
            time.sleep(1)
            klout_score(kloutid)
            time.sleep(1)
            klout_influence(kloutid)
            time.sleep(1)
            klout_topic(kloutid)
            time.sleep(1)
            c = conn.cursor()
            c.execute(sql_klout_twitter_code_update, (1, tw_id))
            conn.commit()
            c.close()
            return kloutid
    except urllib2.HTTPError, e:
        if e.code == 404: ## Not Found
            print e
            c = conn.cursor()
            c.execute(sql_klout_twitter_code_update, (e.code, tw_id))
            conn.commit()
            c.close()
        return None

sql_klout_name_update = '''
UPDATE klout_user SET klout_name = ?, klout_score = ? WHERE klout_id = ?
'''
sql_klout_score_insert = '''
INSERT OR REPLACE INTO klout_score (klout_id, klout_score, klout_scoredelta, create_date) VALUES (?,?,?, date('now'))
'''
def klout_score(kloutid):
    c = conn.cursor()
    url = 'http://api.klout.com/v2/user.json/%s?key=u9cvz5rjmrmskzm6gmsnk9ps'%kloutid
    try:
        web = web_openner.open(url).read()
        js = json.loads(web)
        if js == None:
            return 
        if js.has_key('nick'):
            klout_name = js['nick']
        else:
            klout_name = None
        if js.has_key('score') and js['score'].has_key('score'):
            score = js['score']['score']
        else:
            score = None
        if js.has_key('scoreDelta'):
            scoreDelta = js['scoreDelta']
            scoreDelta = json.dumps(scoreDelta)
        else:
            scoreDelta = None
        c.execute(sql_klout_name_update, (klout_name, score, kloutid))
        conn.commit()
        c.execute(sql_klout_score_insert, (kloutid, score, scoreDelta))
        conn.commit()
        print "score:", klout_name, score, scoreDelta
        #DB
    except urllib2.HTTPError, e:
        print e
    c.close()

sql_klout_influence_insert = '''
INSERT OR REPLACE INTO klout_influence (klout_id, influence_klout_id, create_date, type) VALUES (?, ?, date('now'), ?)
'''
sql_klout_influencer_count_update = '''
UPDATE klout_influence_count SET influencer_count = ? WHERE klout_id = ?
'''
sql_klout_influencee_count_update = '''
UPDATE klout_influence_count SET influencee_count = ? WHERE klout_id = ?
'''
def klout_influence(kloutid):
    c = conn.cursor()
    url = 'http://api.klout.com/v2/user.json/%s/influence?key=u9cvz5rjmrmskzm6gmsnk9ps'%kloutid
    try:
        web = web_openner.open(url).read()
        js = json.loads(web)
        if js.has_key('myInfluencers'):
            influencers = js['myInfluencers']
            for influencer in influencers:
                influencer_id = influencer['entity']['id']
                influencer_score = influencer['entity']['payload']['score']['score']
                influencer_name = influencer['entity']['payload']['nick']
                c.execute(sql_klout_user_insert, (influencer_id, influencer_name, influencer_score))
                conn.commit()
                c.execute(sql_klout_score_insert, (influencer_id, influencer_score, ''))
                conn.commit()
                c.execute(sql_klout_influence_insert, (kloutid, influencer_id, 'influencer'))
                conn.commit()
                print 'influencer:', influencer_id, influencer_score
        if js.has_key('myInfluencees'):
            influencees = js['myInfluencees']
            for influencee in influencees:
                influencee_id = influencee['entity']['id']
                influencee_score = influencee['entity']['payload']['score']['score']
                influencee_name = influencee['entity']['payload']['nick']
                c.execute(sql_klout_user_insert, (influencee_id, influencee_name, influencee_score))
                conn.commit()
                c.execute(sql_klout_score_insert, (influencee_id, influencee_score, ''))
                conn.commit()
                c.execute(sql_klout_influence_insert, (kloutid, influencee_id, 'influencee'))
                conn.commit()
                print 'influencee:', influencee_id, influencee_score
        if js.has_key('myInfluencersCount'):
            influencersCount = js['myInfluencersCount']
            c.execute(sql_klout_influencer_count_update, (influencersCount, kloutid))
            conn.commit()
            print 'influencerCount', influencersCount
        if js.has_key('myInfluenceesCount'):
            influenceesCount = js['myInfluenceesCount']
            c.execute(sql_klout_influencee_count_update, (influenceesCount, kloutid))
            conn.commit()
            print 'influenceeCount', influenceesCount
        #DB
    except urllib2.HTTPError, e:
        print e
    c.close()
sql_klout_topic_insert = '''
INSERT OR IGNORE INTO klout_topic (topic_id, topic_displayname, topic_name) VALUES (?, ?, ?)
'''
sql_klout_topic_user_insert = '''
INSERT OR IGNORE INTO klout_topic_user (topic_id, klout_id) VALUES (?, ?)
'''
def klout_topic(kloutid):
    url = 'http://api.klout.com/v2/user.json/%s/topics?key=u9cvz5rjmrmskzm6gmsnk9ps'%kloutid
    try:
        web = web_openner.open(url).read()
        js = json.loads(web)
        #print js
        for topic in js:
            topic_id = topic['id']
            topic_displayname = topic['displayName']
            topic_name = topic['name']
            c = conn.cursor()
            c.execute(sql_klout_topic_insert, (topic_id, topic_displayname, topic_name))
            conn.commit()
            c.execute(sql_klout_topic_user_insert, (topic_id, kloutid))
            conn.commit()
            c.close()
            print 'topic:', topic_id, topic_displayname, topic_name
        #DB
    except urllib2.HTTPError, e:
        print e


def infochimps_user_list():
    c = conn.cursor()
    c.execute(sql_user_all_get, ())
    for raw in c.fetchall():
        tw_id = raw[0]
        tw_name = raw[1]
        ##
        print "==TW infochimps:", tw_id, tw_name
        get_trstrank(tw_id)
        get_influence_metrics(tw_id)
        time.sleep(1)
        ##
    c.close()

sql_trstrank_insert = '''
INSERT OR REPLACE INTO trstrank (tw_id, tq, trstrank) VALUES (?, ?, ?)
'''
def get_trstrank(tw_id):
    url = 'http://api.infochimps.com/social/network/tw/influence/trstrank?apikey=JianhuaShao-ZzyM911GAwNzJ1F7BEoz1ZGqw69&user_id=%s'%tw_id
    try:
        web = web_openner.open(url).read()
        js = json.loads(web)
        #print js
        if js == None:
            return
        if js.has_key('tq'):
            tq = js['tq']
        else:
            tq = None
        if js.has_key('trstrank'):
            trstrank = js['trstrank']
        else:
            trstrank = None
        c = conn.cursor()
        #print tq, trstrank
        c.execute(sql_trstrank_insert, (tw_id, tq, trstrank))
        conn.commit()
        c.close()
        print "trsttrank", trstrank
    except urllib2.HTTPError, e:
        print e


sql_influence_metrics_insert = '''
INSERT OR REPLACE INTO influence_metrics (tw_id, at_trstrank, chattness, enthusiasm, feedness, fo_trstrank, follow_rate, influx, interesting, outflux, sway) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''
def get_influence_metrics(tw_id):
    url = 'http://api.infochimps.com/social/network/tw/influence/metrics?apikey=JianhuaShao-ZzyM911GAwNzJ1F7BEoz1ZGqw69&user_id=%s'%tw_id
    try:
        web = web_openner.open(url).read()
        js = json.loads(web)
        if js == None:
            return
        at_trstrank = None
        chattness = None
        enthusiasm = None
        feedness = None
        fo_trstrank = None
        follow_rate = None
        influx = None
        interesting = None
        outflux = None
        sway = None
        #print js
        if js.has_key('at_trstrank'):
            at_trstrank = js['at_trstrank']
        if js.has_key('chattness'):
            chattness = js['chattness']
        if js.has_key('enthusiasm'):
            enthusiasm = js['enthusiasm']
        if js.has_key('feedness'):
            feedness = js['feedness']
        if js.has_key('fo_trstrank'):
            fo_trstrank  = js['fo_trstrank']
        if js.has_key('follow_churn'):
            follow_churn = js['follow_churn']
        if js.has_key('follow_rate'):
            follow_rate = js['follow_rate']
        if js.has_key('influx'):
            influx = js['influx']
        if js.has_key('interesting'):
            interesting = js['interesting']
        if js.has_key('outflux'):
            outflux = js['outflux']
        if js.has_key('sway'):
            sway = js['sway']
        c = conn.cursor()
        #print tw_id, at_trstrank, chattness, enthusiasm, feedness, fo_trstrank, follow_rate, influx, interesting, outflux, sway
        c.execute(sql_influence_metrics_insert, (tw_id, at_trstrank, chattness, enthusiasm, feedness, fo_trstrank, follow_rate, influx, interesting, outflux, sway))
        conn.commit()
        c.close()
        print "influence:", at_trstrank
    except urllib2.HTTPError, e:
        print e

sql_trstrank_all_get = '''
SELECT * FROM trstrank
'''
sql_influence_all_get = '''
SELECT * FROM influence_metrics
'''
def infochimps_output():
    f = open('./tweets/infochimps_trstrank.txt', 'w')
    f.write('twitter_id \ttq \ttrstrank\n')
    c = conn.cursor()
    c.execute(sql_trstrank_all_get, ())
    for raw in c.fetchall():
        f.write('%s\t%s\t%s\n'%(raw[0], raw[1], raw[2]))
    c.close()
    f.close()
    ######
    f = open('./tweets/infochimps_influence.txt', 'w')
    f.write('twitter_id \tat_trstrank \tchattness, \tenthusiasm, \tfeedness, \tfo_trstrank, \tfollow_rate, \tinflux, \tinteresting, \toutflux, \tsway')
    c = conn.cursor()
    c.execute(sql_influence_all_get, ())
    for raw in c.fetchall():
        f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n'%(raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7], raw[8], raw[9], raw[10]))
    c.close()
    f.close()

sql_klout_output_all_get = '''
SELECT 
  klout_twitter.tw_id, 
  klout_user.klout_id, 
  klout_user.klout_name, 
  klout_user.klout_score, 
  klout_influence_count.influencer_count, 
  klout_influence_count.influencee_count
FROM klout_user, klout_twitter, klout_influence_count
WHERE 
  klout_twitter.klout_id = klout_user.klout_id AND 
  klout_twitter.klout_id = klout_influence_count.klout_id 
''' 
sql_klout_topic_all_get = '''
SELECT 
  klout_twitter.tw_id,
  klout_twitter.klout_id, 
  klout_topic.topic_id, 
  klout_topic.topic_displayname, 
  klout_topic.topic_name
FROM klout_twitter, klout_topic, klout_topic_user
WHERE 
  klout_twitter.klout_id = klout_topic_user.klout_id AND 
  klout_topic_user.topic_id = klout_topic.topic_id
'''
def klout_output():
    f = open('./tweets/klout_general.txt', 'w')
    f.write('twitter_id \tklout_id \tklout_name \tklout_score \tklout_influencer_count \tklout_influencee_count\n')
    c = conn.cursor()
    c.execute(sql_klout_output_all_get, ())
    for raw in c.fetchall():
        f.write('%s\t%s\t%s\t%s\t%s\t%s\n'%(raw[0], raw[1], raw[2], raw[3], raw[4], raw[5]))
    c.close()
    f.close()
    ######
    f = open('./tweets/klout_topic.txt', 'w')
    f.write('twitter_id \tklout_id \ttopic_id \ttopic_displayname \tktopic_name\n')
    c = conn.cursor()
    c.execute(sql_klout_topic_all_get, ())
    for raw in c.fetchall():
        f.write('%s\t%s\t%s\t%s\t%s\n'%(raw[0], raw[1], raw[2], raw[3], raw[4]))
    c.close()
    f.close()
    


if __name__ == "__main__":
    #twitter_id_to_screenname('15214291')
    #twitter_screenname_to_id('bittercoder')
    #twitter_id_match('./tweets/twitter-data v4-organisation.txt')
    #twitter_id_match('./tweets/twitter-data v4-analyst.txt')
    #######
    db_init()
    tw_init('./tweets/twitter-data v4-organisation.txt.output.txt', tw_type_organisation)
    tw_init('./tweets/twitter-data v4-analyst.txt.output.txt', tw_type_analyst)
    twitter_api_read_main()
    #######
    #twitter_api_read('376193838')
    #tw_get_id_update('22811613', 90, 5)
    ########
    all_status_to_txt()
    ######
    #klout_user_list()
    #get_kloutid('56335495')
    #####
    infochimps_user_list()
    #infochimps_output()
    klout_output()
    
