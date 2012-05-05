import db_util as dbutil
from db_util import conn
from datetime import datetime
import codecs
import sqlite3

file_name = 'EventStudyTwitter20120422'
persona_db = './persona/%s.db'%file_name
company_list_file = './persona/%s.txt'%file_name
conn_clean_path = './data_clean/main.db'
conn_clean = sqlite3.connect(conn_clean_path)

def test():
    print "hello persona"


''' # for test of database speed
SELECT checkin.u_id, checkin.createdat, tweet.stm, tweet.tweet_id, tweet.tweet
FROM  
(SELECT stm_id AS stm, twitter_id AS tweet_id, twitter_text AS tweet
FROM twitter_world_simple 
 WHERE twitter_text LIKE '%google%'  ) AS tweet,
checkin_data AS checkin
WHERE tweet.tweet_id = checkin.checkin.tweet_id 
'''


'''
SELECT stm_id AS stm, twitter_id AS tweet_id, twitter_text AS tweet, 
( SELECT createdat FROM checkin_data WHERE checkin_data.tweet_id = twitter_id
)
FROM twitter_world_simple 
 WHERE twitter_text LIKE '%google%'
'''


sql_filter_with_company_list_world_without_date = '''
SELECT stm_id AS stm, twitter_id AS tweet_id, twitter_text AS tweet, l_id AS language 
FROM twitter_world_simple 
%s 
'''
sql_filter_with_company_list_world = '''
SELECT checkin.u_id, checkin.createdat, tweet.stm, tweet.tweet_id, tweet.tweet, tweet.language 
FROM  
(SELECT stm_id AS stm, twitter_id AS tweet_id, twitter_text AS tweet, l_id AS language
FROM twitter_world_simple 
%s ) AS tweet 
INNER JOIN 
(SELECT checkin_data.u_id, checkin_data.createdat, checkin_data.tweet_id 
FROM checkin_data) AS checkin
ON tweet.tweet_id = checkin.tweet_id 
'''
def filter_with_company_list():
    f_list = open(company_list_file)
    #f_list.readlines(1)
    lines = f_list.readline()
    print lines
    print "*** get into while loop **"
    while True:
        lines = f_list.readlines(10)
        if not lines:
            break
        for line in lines:
            #print len(line)
            line = line.strip()
            words = line.split('\t')
            where_sql = ''
            where_column = 'twitter_text'
            words_search_keyword = []
            length = len(words)
            if length < 3:
                raise Exception('list should have more than 3 columns')
            if length >= 3:
                company_id = words[0]
                company_f100_id = words[1]
                company_name = words[2]
                company_name = company_name.strip()
                where_sql = where_sql+str(where_column)+' LIKE \'%'+str(company_name)+'%\' '
                #print where_sql
            if length > 3:
                for word_i in range(3, length):
                    word_sk = words[word_i]
                    word_sk = word_sk.strip()
                    words_search_keyword.append(word_sk)
                    where_sql = where_sql+' OR '+str(where_column)+' LIKE \'%'+str(word_sk)+'%\' '
                #print words_search_keyword
            #print where_sql
            sql = sql_filter_with_company_list_world % (' WHERE '+ where_sql)
            #sql = sql_filter_with_company_list_world_without_date % (' WHERE '+ where_sql)
            print sql
            #sys.error()
            c = conn.cursor()
            c.execute(sql, )
            file_out_name_company = 'd_%s_%s_%s.txt'%(company_id, company_name, str(datetime.now()))
            file_out = codecs.open('./persona/'+file_out_name_company, 'w', encoding='utf-8')
            file_out.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t'%('company_id', 'company_f100_id', 'company_name', 'search_keywords', 'twitter_id', 'tweet_id', 'sentiment', 'tweet', 'language', 'date'))
            file_out.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t'%('company_id', 'company_f100_id', 'company_name', 'search_keywords', 'tweet_id', 'sentiment', 'tweet', 'language'))
            for raw in c.fetchall():
                #print raw
                twitter_id = raw[0]
                tweet_id = raw[3]
                sentiment = raw[2]
                tweet = raw[4]
                language = raw[5]
                date_txt = raw[1]
                msg = '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t'%(company_id, company_f100_id, company_name, str(words_search_keyword), twitter_id, tweet_id, sentiment, tweet, language, date_txt)
                #tweet_id = raw[1]
                #sentiment = raw[0]
                #tweet = raw[2]
                #if isinstance(tweet, str):
                #    tweet = unicode(tweet, 'utf-8')
                #else:
                #    tweet = unicode(tweet)
                #tweet = unicode(raw[2], encoding='utf-8', errors='ignore')
                #language = raw[3]
                #msg = '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t'%(company_id, company_f100_id, company_name, str(words_search_keyword), tweet_id, sentiment, tweet, language)
                if isinstance(msg, str):
                    msg = unicode(tweet, 'utf-8')
                else:
                    msg = unicode(tweet)
                #print msg
                #msg = unicode(msg, encoding='UTF-8', errors='ignore')
                file_out.write(msg)
                #print raw
            file_out.write('********** finish at %s **********'%(str(datetime.now())))
            file_out.close()
            c.close()
            print len(line), length, line
            print "***************"
    f_list.close()


###################### full #######################

def read_company_list_line(line):
    line = line.strip()
    words = line.split('\t')
    where_sql = ''
    where_column = 'twitter_text'
    words_search_keyword = []
    length = len(words)
    if length < 3:
        raise Exception('list should have more than 3 columns')
    if length >= 3:
        company_id = words[0]
        company_f100_id = words[1]
        company_name = words[2]
        company_name = company_name.strip()
        where_sql = where_sql+str(where_column)+' LIKE \'%'+str(company_name)+'%\' '
                #print where_sql
    if length > 3:
        for word_i in range(3, length):
            word_sk = words[word_i]
            word_sk = word_sk.strip()
            words_search_keyword.append(word_sk)
            where_sql = where_sql+' OR '+str(where_column)+' LIKE \'%'+str(word_sk)+'%\' '
                #print words_search_keyword
    print "reading company (%s): %s"%(str(company_id), company_name)
    return company_id, company_f100_id, company_name, where_sql, words_search_keyword

def sent_world_sql_query(sql):
    sent_world_dict = {}
    c = conn.cursor()
    c.execute(sql, )
    print "**getting into sent_world.sql"
    for raw in c.fetchall():
        tweet_id = raw[1]
        sentiment = raw[0]
        tweet = raw[2]
        if isinstance(tweet, str):
            tweet = unicode(tweet, 'utf-8')
        else:
            tweet = unicode(tweet)
                #tweet = unicode(raw[2], encoding='utf-8', errors='ignore')
        language = raw[3]
        sw = {'sent':sentiment, 'tweet':tweet, 'lang':language}
                #print sw
        sent_world_dict[tweet_id] = sw
    c.close()
    print "**finishing sent_world.sql"
    return sent_world_dict

def company_list_like(keys):
    tweet_id_list = '('
    is_start = True
    for tweet_id in keys: 
        tweet_id_txt = ''
        if is_start == True:
            tweet_id_txt = "%s"%(str(tweet_id))
            is_start = False
        else:
            tweet_id_txt = ', %s'%(str(tweet_id))
                #print tweet_id_txt
        tweet_id_list = tweet_id_list + tweet_id_txt
    tweet_id_list = tweet_id_list + ')'
    return tweet_id_list

def checkin_data_sql_query(tweet_id_list):
    checkin_data_dict = {}
    s_full = sql_full % (tweet_id_list)
    c_full = conn_clean.cursor()
    c_full.execute(s_full, ())
    print "==getting into checkin_data.sql"
    for row in c_full.fetchall():
        #print row
        tweet_id = row[0]
        twitter_id = row[1]
        twitter_time = row[2]
        cd = {'u_id':twitter_id, 'date':twitter_time}
        checkin_data_dict[tweet_id] = cd
            #print sent_world_dict
    c_full.close()
    print "==finishing checkin_data.sql"
    return checkin_data_dict


sql_full = '''
SELECT tweet_id, u_id, createdat FROM checkin_data WHERE tweet_id in %s
'''

def read_line_loop_body(line):
    company_id, company_f100_id, company_name, where_sql, words_search_keyword = read_company_list_line(line)
#print where_sql
    sql = sql_filter_with_company_list_world_without_date % (' WHERE '+ where_sql)
            #print sql
            #sys.error()
    sent_world_dict = sent_world_sql_query(sql)
    keys = sent_world_dict.keys()
    tweet_id_list = company_list_like(keys)
            #print tweet_id_list, " tweet id list"
            #print sql_full
    checkin_data_dict = checkin_data_sql_query(tweet_id_list)
            #print checkin_data_dict
    file_out_name_company = 'full_d_%s_%s_%s.txt'%(company_id, company_name, str(datetime.now()))
    file_out = codecs.open('./persona/'+file_out_name_company, 'w', encoding='utf-8')
    file_out.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n'%('company_id', 'company_f100_id', 'company_name', 'search_keywords', 'user_id', 'tweet_id', 'sentiment', 'language', 'date', 'tweet'))
    for tweet_id in sent_world_dict:
        d = sent_world_dict[tweet_id]
                #print d
        sent = d['sent'].strip()
        tweet = d['tweet'].strip()
        lang = d['lang'].strip()
        if not checkin_data_dict.has_key(tweet_id):
            u_id = 'not_appear'
            date_txt = 'not_appear'
        else:
            u_id = checkin_data_dict[tweet_id]['u_id']
            date_txt = checkin_data_dict[tweet_id]['date'].strip()
        print "hello:>>", u_id, tweet_id, sent, lang, date_txt, tweet
        msg = '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n'%(company_id, company_f100_id, company_name, str(words_search_keyword), u_id, tweet_id, sent, lang, date_txt, tweet)
                #if isinstance(msg, str):
                #    msg = unicode(msg, 'utf-8')
                #else:
                #    msg = unicode(msg)
                #print msg
                #msg = unicode(msg, encoding='UTF-8', errors='ignore')
        file_out.write(msg)
                #print raw
    file_out.write('********** finish at %s **********'%(str(datetime.now())))
    file_out.close()
    print len(line), line
    print "***************"

def filter_with_company_list_full():
    f_list = open(company_list_file)
    #f_list.readlines(1)
    lines = f_list.readline()
    print lines
    print "*** get into while loop **"
    while True:
        lines = f_list.readlines(1)
        if not lines:
            break
        for line in lines:
            #print len(line)
            read_line_loop_body(line)
    f_list.close()
    
conn_persona = sqlite3.connect(persona_db)
persona_db_sql_init = '''
DROP TABLE IF EXISTS persona_company;
CREATE TABLE IF NOT EXISTS persona_company (
  fortune_id INTEGER NOT NULL,
  company_name TEXT NOT NULL,
  where_sql TEXT,
  search_keywords TEXT
);
'''
def persona_db_init():
    c = conn_persona.cursor()
    c.executescript(persona_db_sql_init)
    conn_persona.commit()
    c.close()

persona_db_sql_company_meta_insert = '''
INSERT INTO persona_company (fortune_id, company_name, where_sql, search_keywords) VALUES (?, ?, ?, ?)
'''
def personal_db_company_meta_insert(fortune_id, company_name, company_search_keywords, company_where_sql):
    c = conn_persona.cursor()
    #company_search_keywords
    param = (fortune_id, company_name, company_where_sql, str(company_search_keywords))
    c.execute(persona_db_sql_company_meta_insert, param)
    conn_persona.commit()
    c.close()
  
def company_table_name_get(fortune_id, company_name):
    c_name = company_name.replace(" ", "_")
    c_name = c_name.replace("-", "_")
    c_name = c_name.replace("&", "_")
    c_name = c_name.replace(".", "")
    table_name = 'c_%s_%s'%(str(fortune_id), c_name) 
    return table_name

persona_db_sql_company_table_init = '''
DROP TABLE IF EXISTS %s;
CREATE TABLE IF NOT EXISTS %s (
  fortune_id INTEGER NOT NULL,
  user_id TEXT, 
  tweet_id TEXT, 
  sentiment TEXT, 
  language TEXT, 
  date TEXT, 
  tweet TEXT
);
'''
def persona_db_company_table_init(company_table_name):
    sql = persona_db_sql_company_table_init % (company_table_name, company_table_name)
    #print persona_db_sql_company_table_init
    #print sql
    c = conn_persona.cursor()
    c.executescript(sql)
    conn_persona.commit()
    c.close()
persona_db_sql_company_table_insert = '''
INSERT INTO %s (fortune_id, user_id, tweet_id, sentiment, language, date, tweet) VALUES (?, ?, ?, ?, ?, ?, ?)
'''
#def personal_db_company_table_insert(company_table_name, fortune_id, user_id, tweet_id, sentiment, language, date, tweet):
#    sql = persona_db_sql_company_table_insert % company_table_name
#    c = conn.persona.cu

def read_company_line(line):
    line = line.strip()
    words = line.split('\t')
    where_sql = ''
    where_column = 'twitter_text'
    words_search_keyword = []
    length = len(words)
    if length < 2:
        raise Exception('list should have more than 2 columns')
    if length >= 2:
        company_f100_id = words[0]
        company_name = words[1]
        company_name = company_name.strip()
        where_sql = where_sql+str(where_column)+' LIKE \'%'+str(company_name)+'%\' '
                #print where_sql
    if length > 2:
        for word_i in range(2, length):
            word_sk = words[word_i]
            word_sk = word_sk.strip()
            words_search_keyword.append(word_sk)
            where_sql = where_sql+' OR '+str(where_column)+' LIKE \'%'+str(word_sk)+'%\' '
                #print words_search_keyword
    print "reading company (%s): %s"%(str(company_f100_id), company_name)
    #print company_f100_id, company_name, where_sql, words_search_keyword
    return company_f100_id, company_name, where_sql, words_search_keyword

def filter_company_without_date(fortune_id, company_name, where_sql):
    company_table_name = company_table_name_get(fortune_id, company_name)
    print 'company_table_name:%s'%company_table_name
    persona_db_company_table_init(company_table_name)
    sql = sql_filter_with_company_list_world_without_date % (' WHERE '+ where_sql)
    sent_world_dict = sent_world_sql_query(sql)
    #print sent_world_dict
    keys = sent_world_dict.keys()
    tweet_id_list = company_list_like(keys)
    checkin_data_dict = checkin_data_sql_query(tweet_id_list)
    sql_company_table_insert = persona_db_sql_company_table_insert % company_table_name
    c = conn_persona.cursor()
    for tweet_id in sent_world_dict:
        d = sent_world_dict[tweet_id]
        sent = d['sent'].strip()
        tweet = d['tweet'].strip()
        lang = d['lang'].strip()
        if not checkin_data_dict.has_key(tweet_id):
            u_id = 'not_appear'
            date_txt = 'not_appear'
        else:
            u_id = checkin_data_dict[tweet_id]['u_id']
            date_txt = checkin_data_dict[tweet_id]['date'].strip()
        params = (fortune_id, u_id, tweet_id, sent, lang, date_txt, tweet)
        #print params
        c.execute(sql_company_table_insert, params)
    c.close()

def event_study_twitter():
    print 'event study start'
    persona_db_init()
    f_list = open(company_list_file, 'rU')
    lines = f_list.readline()
    #print lines, 'cool'
    #print company_list_file
    #if 
    while True:
        lines = f_list.readlines(1)
        if not lines:
            break
        for line in lines:
            #print line.strip()
            fortune_id, company_name, company_where_sql, company_search_keywords = read_company_line(line)
            personal_db_company_meta_insert(fortune_id, company_name, company_search_keywords, company_where_sql)
            filter_company_without_date(fortune_id, company_name, company_where_sql)
    f_list.close()

if __name__ == '__main__':
    event_study_twitter()
    
