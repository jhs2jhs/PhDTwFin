import txt_to_sql as filein
import db_util as dbutil
import persona_filter as persona

def init():
    dbutil.db_create()

def txtin():
    print "sent_uk_full start"
    filein.sentiment_uk_txt_file_read("./data/sentiment/sent_uk.txt")
    print "sent_uk_full end"
    print "sent_world start"
    filein.sentiment_world_txt_file_read("./data/sentiment/sent_world.txt")
    print "sent_world end"
    print "checkin start"
    filein.checkin_txt_file_read("./data/icwsm_2011/checkin_data.txt")
    print "checkin end"
    print "users_data start"
    filein.users_data_txt_file_read("./data/icwsm_2011/users_data.txt")
    print "users_data end"
   

def filting():
    #persona.test()
    persona.filter_with_company_list()

if __name__ == "__main__":
    print "main"
    init()
    txtin()
    #filting()
    print "finish"
    #txtin()
