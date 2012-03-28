import db_util as dbutil
from db_util import conn
from datetime import datetime
import codecs

company_list_file = './persona/list.txt'

def test():
    print "hello persona"


''' # for test of database speed
SELECT checkin.u_id, checkin.createdat, tweet.stm, tweet.tweet_id, tweet.tweet
FROM  
(SELECT stm_id AS stm, twitter_id AS tweet_id, twitter_text AS tweet
FROM twitter_world_simple 
 WHERE twitter_text LIKE '%camdenliving%'  ) AS tweet,
checkin_data AS checkin
WHERE tweet.tweet_id = checkin.checkin.tweet_id 
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
    

