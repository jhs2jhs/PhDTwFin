
import sys
import sqlite3

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
def klout_output(data_source, conn):
    f = open('./'+data_source+'/klout_general.txt', 'w')
    f.write('twitter_id \tklout_id \tklout_name \tklout_score \tklout_influencer_count \tklout_influencee_count\n')
    c = conn.cursor()
    c.execute(sql_klout_output_all_get, ())
    for raw in c.fetchall():
        f.write('%s\t%s\t%s\t%s\t%s\t%s\n'%(raw[0], raw[1], raw[2], raw[3], raw[4], raw[5]))
    c.close()
    f.close()
    ######
    f = open('./'+data_source+'/klout_topic.txt', 'w')
    f.write('twitter_id \tklout_id \ttopic_id \ttopic_displayname \tktopic_name\n')
    c = conn.cursor()
    c.execute(sql_klout_topic_all_get, ())
    for raw in c.fetchall():
        f.write('%s\t%s\t%s\t%s\t%s\n'%(raw[0], raw[1], raw[2], raw[3], raw[4]))
    c.close()
    f.close()
    print "job done"

if __name__ == "__main__":
    print "CMD: format: python klout_output_txt.py [datasource]"
    cmds = sys.argv
    if len(cmds) != 2:
        print "ERROR: please follow the CMD format"
        print "CMD: please try again"
    else:
        if cmds[1] == "100company":
            print "working on 100company datasoruce"
            data_source = '100company'
            conn = sqlite3.connect('./100company/tweet_100company.db')
            klout_output(data_source, conn)
        elif cmds[1] == "twibes":
            print "working on twibes datasoruce"
            data_source = 'twibes'
            conn = sqlite3.connect('./twibes/tweet_twibes.db')
            klout_output(data_source, conn)
        elif cmds[1] == "ting_brokers":
            print "working on ting_brokers datasoruce"
            data_source = 'ting_brokers'
            conn = sqlite3.connect('./ting_brokers/tweet_tingbrokers.db')
            klout_output(data_source, conn)
        elif cmds[1] == "ting_analysts":
            print "working on ting_analysts datasoruce"
            data_source = 'ting_analysts'
            conn = sqlite3.connect('./ting_analysts/tweet_tinganalysts.db')
            klout_output(data_source, conn)
        else:
            print "ERROR: [datasource] option can only be 100company or twibes"
