import sys
import codecs
import sqlite3


sql_status_get_all = '''
SELECT * FROM review
'''
def all_status_to_txt(open_path, conn):
    f = codecs.open(open_path, mode='w', encoding='utf-8')
    f.write('review_id \tapp_id \treviewer \tdate \tdevice \tversion \ttitle \tcomment \treview_score \t\n')
    c = conn.cursor()
    c.execute(sql_status_get_all, ())
    for row in c.fetchall():
        review_id = row[0].strip().replace('\n', '').replace('\r', '')
        app_id = row[1].strip().replace('\n', '').replace('\r', '')
        reviewer = row[2].strip().replace('\n', '').replace('\r', '')
        date = row[3].strip().replace('\n', '').replace('\r', '')
        device = row[4].strip().replace('\n', '').replace('\r', '')
        version = row[5].strip().replace('\n', '').replace('\r', '')
        title = row[6].strip().replace('\n', '').replace('\r', '')
        comment = row[7].strip().replace('\n', '').replace('\r', '')
        review_score = row[8].strip().replace('\n', '').replace('\r', '')
        print review_id, version
        f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n'%(review_id, app_id, reviewer, date, device, version, title, comment, review_score))
    c.close()
    f.close()

import random
sql_status_get_all_short_percent_random = '''
SELECT DISTINCT(app_id) FROM review
'''
sql_status_get_all_short_percent_by_id_random = '''
SELECT * FROM review WHERE app_id = ? LIMIT ?
'''
def all_status_to_txt_short_most_recent(open_path, conn, count):
    f = codecs.open(open_path, mode='w', encoding='utf-8')
    f.write('review_id \tapp_id \treviewer \tdate \tdevice \tversion \ttitle \tcomment \treview_score \t\n')
    app_ids = []
    c = conn.cursor()
    c.execute(sql_status_get_all_short_percent_random, ())
    for r in c.fetchall():
        app_ids.append(r[0])
    print "done app_ids", len(app_ids)
    i = 0
    for app_id in app_ids:
        #print "app_id: ", app_id
        c.execute(sql_status_get_all_short_percent_by_id_random, (app_id, count, ))
        for row in c.fetchall():
            review_id = row[0].strip().replace('\n', '').replace('\r', '')
            app_id = row[1].strip().replace('\n', '').replace('\r', '')
            reviewer = row[2].strip().replace('\n', '').replace('\r', '')
            date = row[3].strip().replace('\n', '').replace('\r', '')
            device = row[4].strip().replace('\n', '').replace('\r', '')
            version = row[5].strip().replace('\n', '').replace('\r', '')
            title = row[6].strip().replace('\n', '').replace('\r', '')
            comment = row[7].strip().replace('\n', '').replace('\r', '')
            review_score = row[8].strip().replace('\n', '').replace('\r', '')
            #print review_id, version, app_id, 
            f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n'%(review_id, app_id, reviewer, date, device, version, title, comment, review_score))
        print "## progress: ", i, "/", len(app_ids), "## app_id: ", app_id
        i = i + 1
    c.close()
    f.close()


import random
sql_status_get_all_short_percent = '''
SELECT DISTINCT(app_id) FROM review
'''
sql_status_get_all_short_percent_by_id = '''
SELECT DISTINCT(review_id) FROM review WHERE app_id = ?
'''
def all_status_to_txt_short_percent(open_path, conn, percent):
    f = codecs.open(open_path, mode='w', encoding='utf-8')
    f.write('review_id \tapp_id \treviewer \tdate \tdevice \tversion \ttitle \tcomment \treview_score \t\n')
    app_ids = []
    c = conn.cursor()
    c.execute(sql_status_get_all_short_percent, ())
    for r in c.fetchall():
        app_ids.append(r[0])
    print "done app_ids", len(app_ids)
    i = 0
    for app_id in app_ids:
        i = i + 1
        if i < 802:
            continue
        c.execute(sql_status_get_all_short_percent_by_id, (app_id, ))
        rds = []
        print "============== start sample collection ======================" 
        for r in c.fetchall():
            rds.append(str(r[0]))
        ls = int(len(rds) * percent * 0.01)
        print '\t', len(rds), '\t', ls, '\t', app_id
        rds = random.sample(rds, ls)
        if len(rds) == 0:
            rds_str = "()"
            continue
        elif len(rds) == 1:
            rds_str = '('+rds[0]+')'
            sql_rds_str = 'SELECT * FROM review WHERE review_id = "%s"'%(str(rds[0]))
        else:
            rds_str = str(tuple(rds))
            sql_rds_str = 'SELECT * FROM review WHERE review_id in %s'%(rds_str)
        #print rds_str
        print sql_rds_str
        print "## done random selection"
        c.execute(sql_rds_str, ())
        for row in c.fetchall():
            review_id = row[0].strip().replace('\n', '').replace('\r', '')
            app_id = row[1].strip().replace('\n', '').replace('\r', '')
            reviewer = row[2].strip().replace('\n', '').replace('\r', '')
            date = row[3].strip().replace('\n', '').replace('\r', '')
            device = row[4].strip().replace('\n', '').replace('\r', '')
            version = row[5].strip().replace('\n', '').replace('\r', '')
            title = row[6].strip().replace('\n', '').replace('\r', '')
            comment = row[7].strip().replace('\n', '').replace('\r', '')
            review_score = row[8].strip().replace('\n', '').replace('\r', '')
            #print review_id, version
            f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n'%(review_id, app_id, reviewer, date, device, version, title, comment, review_score))
            i = i+ 1
        #print i, 'haha'
        print "## progress: ", i, "/", len(app_ids), "## app_id: ", app_id
    c.close()
    f.close()



if __name__ == "__main__":
    print "CMD: format: python app_review_output_txt.py [all | recent | random] [percentage | number]"
    cmds = sys.argv
    if len(cmds) != 3:
        print "ERROR: please follow the CMD format"
        print "CMD: please try again"
    else:
        print "working on db_app_review.db datasoruce"
        open_path = "./db_app_review_"
        conn = sqlite3.connect('./db_app_review.db')
        if cmds[1] == "all":
            print "output all"
            open_path = open_path+"all.txt"
            all_status_to_txt(open_path, conn)
        elif cmds[1] == "recent":
            print "output recent", cmds[2]
            count = int(cmds[2])
            open_path = open_path+"recent_"+cmds[2]+".txt"
            all_status_to_txt_short_most_recent(open_path, conn, count)
        elif cmds[1] == "random":
            print "output random", cmds[2]
            percentage = int(cmds[2])
            open_path = open_path+"random_"+cmds[2]+".txt"
            all_status_to_txt_short_percent(open_path, conn, percentage)
        else:
            print "ERROR has to be one of [all | recent | random]"
    
