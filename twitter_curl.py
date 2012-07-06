import urllib2
from BeautifulSoup import BeautifulSoup
import sqlite3
import json

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
  create_time TIMESTAMP DEFAULT (DATETIME('now')),
  FOREIGN KEY (tw_id) REFERENCES tw(tw_id)
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
UPDATE tw_get SET min_finish = 1 WHERE tw_id = ?
'''
def db_get_update(type, tw_id, id):
    c = conn.cursor()
    if type == 'max':
        c.execute(sql_get_update_max, (id, tw_id))
    if type == 'min':
        c.execute(sql_get_update_min, (id, tw_id))
    if type == 'min_finish':
        c.execute(sql_get_update_min_finish, (tw_id, ))
    conn.commit()
    c.close()

sql_status_insert = '''
INSERT OR IGNORE INTO tw_status (tid, tw_text, source, reply_to_status, reply_to_user, reply_to_username, geo, coordinates, place, contributors, retweet_count, favorited, retweeted, possibly_sensitive, retweet_status_id, hashtags, urls, user_mentions, tw_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
'''
def db_status_insert(tid, tw_text, source, reply_to_status, reply_to_user, reply_to_username, geo, coordinates, place, contributors, retweet_count, favorited, retweeted, possibly_sensitive, retweet_status_id, hashtags, urls, user_mentions, tw_id):
    c = conn.cursor()
    c.execute(sql_status_insert, (tid, tw_text, source, reply_to_status, reply_to_user, reply_to_username, geo, coordinates, place, contributors, retweet_count, favorited, retweeted, possibly_sensitive, retweet_status_id, hashtags, urls, user_mentions, tw_id))
    conn.commit()
    c.close()


def twitter_response_parse(html):
    js = json.loads(html)
    max_id = 0
    min_id = 0
    tw_id = 0
    i = 0
    for j in js:
        created_at = j['created_at']
        tid = j['id']
        tw_text = j['text'] ##
        source = j['source']
        reply_to_status = j['in_reply_to_status_id']
        reply_to_user = j['in_reply_to_user_id']
        reply_to_username = j['in_reply_to_screen_name']
        geo = j['geo']
        coordinates = j['coordinates']
        place = j['place']
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
        tw_id = user['id']
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
        db_status_insert(tid, tw_text, source, reply_to_status, reply_to_user, reply_to_username, geo, coordinates, place, contributors, retweet_count, favorited, retweeted, possibly_sensitive, retweet_status_id, hashtags, urls, user_mentions, tw_id)
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
    print tw_id
    if tw_id != 0:
        tw_get_id_update(tw_id, max_id, min_id)
        tw_get_min_finish_check(tw_id)

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
        db_get_update('min_finish', tw_id, 0)
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
        html = web_openner.open(url).read()
    # ready to parse error 
        twitter_response_parse(html)
        if loop_type == 'min': ######### if only cover min at moment
            twitter_read_loop_min(tw_id)
    except urllib2.HTTPError, e:
        print "HttpError:"+tw_id, 
        print e.code, e
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
    if min_finish == 0:
        c.execute(sql_get_min_get, (tw_id, ))
        raw = c.fetchone()
        min_id = raw[0]
        url = 'https://api.twitter.com/1/statuses/user_timeline.json?include_entities=true&include_rts=true&user_id=%s&count=199&max_id=%s'%(tw_id, min_id)
        twitter_api_read(url, tw_id, 'min')
    c.close()

def twitter_read(tw_id):
    print "**"+tw_id
    c = conn.cursor()
    c.execute(sql_get_min_get, (tw_id, ))
    raw = c.fetchone()
    min_id = raw[0]
    ########### better to check whether it should loop from max or min
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
        #if i == 100:
        #    break
        #i = i+1
        print i
    c.close()


if __name__ == "__main__":
    #twitter_id_to_screenname('15214291')
    #twitter_screenname_to_id('bittercoder')
    #twitter_id_match('./tweets/twitter-data v4-organisation.txt')
    #twitter_id_match('./tweets/twitter-data v4-analyst.txt')
    db_init()
    tw_init('./tweets/twitter-data v4-organisation.txt.output.txt', tw_type_organisation)
    tw_init('./tweets/twitter-data v4-analyst.txt.output.txt', tw_type_analyst)
    #twitter_api_read('376193838')
    #tw_get_id_update('22811613', 90, 5)
    twitter_api_read_main()
    
