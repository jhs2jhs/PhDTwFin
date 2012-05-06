import txt_to_sql as filein
import db_util as dbutil
import persona_filter as persona

def init():
    dbutil.db_create()

def txtin():
    print "checkin start"
    #filein.checkin_txt_file_read("./data/icwsm_2011/checkin_data.txt")
    filein.checkin_txt_file_read_one("./data/icwsm_2011/checkin_data.txt")
    print "checkin end"
    print "sent_world start"
    #filein.sentiment_world_txt_file_read_one("./data/sentiment/sent_world.txt")
    print "sent_world end"
    print "sent_uk_full start"
    #filein.sentiment_uk_txt_file_read("./data/sentiment/sent_uk.txt")
    print "sent_uk_full end"
    print "users_data start"
    #filein.users_data_txt_file_read("./data/icwsm_2011/users_data.txt")
    print "users_data end"
   

def filting():
    #persona.test()
    #persona.filter_with_company_list() # no user id and date
    #persona.filter_with_company_list_full()
    persona.event_study_twitter_to_db()
    persona.event_study_twitter_to_file_full()
    persona.persona_db_unique_user_id()
    persona.persona_db_unique_user_name()

if __name__ == "__main__":
    print "main"
    #init()
    #txtin()
    filting()
    print "finish"
    #txtin()
