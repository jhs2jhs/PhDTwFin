import db_util as dbutil

readlines_number = 10000

def sentiment_uk_txt_file_read(filename):
    f = open(filename)
    while True:
        lines = f.readlines(readlines_number)
        if not lines:
            break
        words_list = []
        for l in lines:
            words = l.split("\t")
            if len(words) != 4:
                print len(words), words
            sentiment = words[0]
            twitter_stm = unicode(words[1], encoding='UTF-8')
            twitter_id = words[2]
            language = unicode(words[3], encoding='UTF-8').strip('\n')
            word = (sentiment, twitter_stm, twitter_id, language)
            words_list.append(word)
        #dbutil.bulk_insert(words_list, dbutil.conn_uk, dbutil.sql_insert_words_uk_list)
        dbutil.bulk_insert(words_list, dbutil.conn, dbutil.sql_insert_words_uk_list)
    f.close()

def sentiment_world_txt_file_read(filename):
    f = open(filename)
    while True:
        lines = f.readlines(readlines_number)
        if not lines:
            break
        words_list = []
        for l in lines:
            words = l.split("\t")
            if len(words) != 4:
                print len(words), words
            sentiment = words[0]
            twitter_stm = unicode(words[1], encoding='UTF-8')
            twitter_id = words[2]
            language = unicode(words[3], encoding='UTF-8').strip('\n')
            word = (sentiment, twitter_stm, twitter_id, language)
            words_list.append(word)
        #dbutil.bulk_insert(words_list, dbutil.conn_world, dbutil.sql_insert_words_world_list)
        dbutil.bulk_insert(words_list, dbutil.conn, dbutil.sql_insert_words_world_list)
    f.close()

def users_data_txt_file_read(filename):
    f = open(filename)
    while True:
        lines = f.readlines(readlines_number)
        if not lines:
            break
        words_list = []
        for l in lines:
            words = l.split("\t")
            if len(words) != 4:
                print len(words), words
            u_id = words[0]
            status_count = words[1]
            followers_count = words[2]
            friends_count = unicode(words[3], encoding='UTF-8').strip('\n')
            word = (u_id, status_count, followers_count, friends_count)
            words_list.append(word)
        #dbutil.bulk_insert(words_list, dbutil.conn_userdata, dbutil.sql_insert_words_users_data_list)
        dbutil.bulk_insert(words_list, dbutil.conn, dbutil.sql_insert_words_users_data_list)
    f.close()


def checkin_txt_file_read(filename):
    f = open(filename)
    index = 0
    while True:
        lines = f.readlines(readlines_number)
        words_list = []
        #print "index: %d"%(index)
        index = index + 1 
        if not lines:
            break
        for l in lines:
            words = l.split("\t") 
            if len(words) != 7:
                print len(words), words
                #raise Exception
            #print words
            u_id = unicode(words[0], encoding='UTF-8')
            tweet_id = unicode(words[1], encoding='UTF-8')
            latitude = unicode(words[2], encoding='UTF-8')
            longitude = unicode(words[3], encoding='UTF-8')
            createdat = unicode(words[4], encoding='UTF-8')
            text = unicode(words[5], encoding='UTF-8')
            place_id = unicode(words[6], encoding='UTF-8').strip('\n')
            word = (u_id, tweet_id, latitude, longitude, createdat, text, place_id)
            #print word
            words_list.append(word)
        #dbutil.bulk_insert(words_list, dbutil.conn_checkin, dbutil.sql_insert_words_checkin_list)
        dbutil.bulk_insert(words_list, dbutil.conn, dbutil.sql_insert_words_checkin_list)
    f.close()



'''
def checkin_txt_file_read(filename):
    f = open(filename)
    lines = f.readlines()
    for l in lines:
        words = l.split("\t") 
        if len(words) != 7:
            print words
            print len(words)
        dbutil.checkin_txt_to_db(words)
'''

if __name__ == "__main__":
    print "txt_to_sql"
