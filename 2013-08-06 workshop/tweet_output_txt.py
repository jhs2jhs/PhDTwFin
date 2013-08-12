import sys
import codecs
import sqlite3


sql_status_get_all = '''
SELECT * FROM tw_status
'''
def all_status_to_txt(open_path, conn):
    f = codecs.open(open_path, mode='w', encoding='utf-8')
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
        #print tw_id, tw_text, tid, tw_create_at, retweet_count, favorited, retweet, hashtags, urls, user_mentions, reply_to_status, reply_to_status, reply_to_user, geo, coordinates, place
        print tid
        f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n'%(tw_id, tw_text, tid, tw_create_at, retweet_count, favorited, retweet, hashtags, urls, user_mentions, reply_to_status, reply_to_user, geo, coordinates, place))
    c.close()
    f.close()


sql_status_get_all = '''
SELECT * FROM tw_status ORDER BY tw_id
'''
def all_status_to_txt_short_most_recent(open_path, conn, count):
    f = codecs.open(open_path, mode='w', encoding='utf-8')
    f.write('twitter_id \ttweet \ttweet_id \ttweet_create_at \tretweet_count \tfavorited_count \tretweet \thashtags \turls \tuser_mentions \treplay_to_status \treply_to_user \tgeo \tcoordinates \tplace\n')
    c = conn.cursor()
    c.execute(sql_status_get_all, ())
    id_pre = ''
    i = 0
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
        i = i+ 1
        #print i
        if tw_id != id_pre:
            id_pre = tw_id
            i = 0
        if i>= count:
            continue
        #print tw_id, tw_text, tid, tw_create_at, retweet_count, favorited, retweet, hashtags, urls, user_mentions, reply_to_status, reply_to_status, reply_to_user, geo, coordinates, place
        print tid
        f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n'%(tw_id, tw_text, tid, tw_create_at, retweet_count, favorited, retweet, hashtags, urls, user_mentions, reply_to_status, reply_to_user, geo, coordinates, place))
    c.close()
    f.close()


import random
sql_status_get_all_short_percent = '''
SELECT DISTINCT(tw_id) FROM tw_status
'''
sql_status_get_all_short_percent_by_id = '''
SELECT DISTINCT(tid) FROM tw_status WHERE tw_id = ?
'''
def all_status_to_txt_short_percent(open_path, conn, percent):
    f = codecs.open(open_path, mode='w', encoding='utf-8')
    f.write('twitter_id \ttweet_id \ttweet \ttweet_create_at \tretweet_count \tfavorited_count \tretweet \thashtags \turls \tuser_mentions \treplay_to_status \treply_to_user \tgeo \tcoordinates \tplace\n')
    tw_ids = []
    c = conn.cursor()
    c.execute(sql_status_get_all_short_percent, ())
    for r in c.fetchall():
        tw_ids.append(r[0])
    for tw_id in tw_ids:
        c.execute(sql_status_get_all_short_percent_by_id, (tw_id, ))
        rds = []
        for r in c.fetchall():
            rds.append(int(r[0]))
        ls = int(len(rds) * percent * 0.01)
        print '\t', len(rds), '\t', ls, '\t', tw_id
        rds = random.sample(rds, ls)
        if len(rds) == 1:
            rds_str = '('+str(rds[0])+')'
        else:
            rds_str = str(tuple(rds))
        #print rds_str
        sql_rds_str = 'SELECT * FROM tw_status WHERE tid in %s'%(rds_str)
        c.execute(sql_rds_str, ())
        i = 0
        for raw in c.fetchall():
            tid = raw[1]
            tw_text = raw[2].strip().replace('\n', '').replace('\r', '').replace('\t', '')
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
            i = i+ 1
        #print tw_id, tw_text, tid, tw_create_at, retweet_count, favorited, retweet, hashtags, urls, user_mentions, reply_to_status, reply_to_status, reply_to_user, geo, coordinates, place
            print tid
            f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n'%(tw_id, tid, tw_text, tw_create_at, retweet_count, favorited, retweet, hashtags, urls, user_mentions, reply_to_status, reply_to_user, geo, coordinates, place))
        #print i, 'haha'
    c.close()
    f.close()



if __name__ == "__main__":
    print "CMD: format: python output_txt.py [datasource] [all | recent | random] [percentage | number]"
    cmds = sys.argv
    if len(cmds) != 4:
        print "ERROR: please follow the CMD format"
        print "CMD: please try again"
    else:
        if cmds[1] == "100company":
            print "working on 100company datasoruce"
            open_path = "./100company/tweet_status_100company_"
            conn = sqlite3.connect('./100company/tweet_100company.db')
            if cmds[2] == "all":
                print "output all"
                open_path = open_path+"all.txt"
                all_status_to_txt(open_path, conn)
            elif cmds[2] == "recent":
                print "output recent", cmds[3]
                count = int(cmds[3])
                open_path = open_path+"recent_"+cmds[3]+".txt"
                all_status_to_txt_short_most_recent(open_path, conn, count)
            elif cmds[2] == "random":
                print "output random", cmds[3]
                percentage = int(cmds[3])
                open_path = open_path+"random_"+cmds[3]+".txt"
                all_status_to_txt_short_percent(open_path, conn, percentage)
            else:
                print "ERROR has to be one of [all | recent | random]"
        elif cmds[1] == "twibes":
            print "working on twibes datasoruce"
            open_path = "./twibes/tweet_status_twibes_"
            conn = sqlite3.connect('./twibes/tweet_twibes.db')
            if cmds[2] == "all":
                print "output all"
                open_path = open_path+"all.txt"
                all_status_to_txt(open_path, conn)
            elif cmds[2] == "recent":
                print "output recent", cmds[3]
                count = int(cmds[3])
                open_path = open_path+"recent_"+cmds[3]+".txt"
                all_status_to_txt_short_most_recent(open_path, conn, count)
            elif cmds[2] == "random":
                print "output random ", cmds[3]
                percentage = int(cmds[3])
                open_path = open_path+"random_"+cmds[3]+".txt"
                all_status_to_txt_short_percent(open_path, conn, percentage)
            else:
                print "ERROR has to be one of [all | recent | random]"
        elif cmds[1] == "ting_brokers":
            print "working on ting_brokers datasoruce"
            open_path = "./ting_brokers/tweet_status_tingbrokers_"
            conn = sqlite3.connect('./ting_brokers/tweet_tingbrokers.db')
            if cmds[2] == "all":
                print "output all"
                open_path = open_path+"all.txt"
                all_status_to_txt(open_path, conn)
            elif cmds[2] == "recent":
                print "output recent", cmds[3]
                count = int(cmds[3])
                open_path = open_path+"recent_"+cmds[3]+".txt"
                all_status_to_txt_short_most_recent(open_path, conn, count)
            elif cmds[2] == "random":
                print "output random ", cmds[3]
                percentage = int(cmds[3])
                open_path = open_path+"random_"+cmds[3]+".txt"
                all_status_to_txt_short_percent(open_path, conn, percentage)
            else:
                print "ERROR has to be one of [all | recent | random]"
        elif cmds[1] == "ting_analysts":
            print "working on ting_analysts datasoruce"
            open_path = "./ting_analysts/tweet_status_tinganalysts_"
            conn = sqlite3.connect('./ting_analysts/tweet_tinganalysts.db')
            if cmds[2] == "all":
                print "output all"
                open_path = open_path+"all.txt"
                all_status_to_txt(open_path, conn)
            elif cmds[2] == "recent":
                print "output recent", cmds[3]
                count = int(cmds[3])
                open_path = open_path+"recent_"+cmds[3]+".txt"
                all_status_to_txt_short_most_recent(open_path, conn, count)
            elif cmds[2] == "random":
                print "output random ", cmds[3]
                percentage = int(cmds[3])
                open_path = open_path+"random_"+cmds[3]+".txt"
                all_status_to_txt_short_percent(open_path, conn, percentage)
            else:
                print "ERROR has to be one of [all | recent | random]"
        else:
            print "ERROR: [datasource] option can only be 100company or twibes"
    
