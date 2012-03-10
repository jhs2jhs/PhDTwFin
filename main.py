import txt_to_sql as filein
import db_util as dbutil

def txtin():
    print "sent_uk_full start"
    filein.sentiment_txt_file_read("./data/sentiment/sent_uk.txt", "uk")
    print "sent_uk_full end"
    print "sent_world start"
    filein.sentiment_txt_file_read("./data/sentiment/sent_world.txt", "world")
    print "sent_world end"
    print "users_data start"
    filein.users_data_txt_file_read("./data/icwsm_2011/users_data.txt")
    print "users_data end"
    print "checkin start"
    filein.checkin_txt_file_read("./data/icwsm_2011/checkin_data.txt")
    print "checkin end"

if __name__ == "__main__":
    print "main"
    dbutil.db_create()
    txtin()
    #txtin()
