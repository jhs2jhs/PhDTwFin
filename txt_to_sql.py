import db_util as dbutil
def sentiment_txt_file_read(filename, level):
    f = open(filename)
    lines = f.readlines()
    for l in lines:
        #print l
        words = l.split("\t")
        #print len(words)
        if len(words) != 4:
            print words
            print len(words)
        dbutil.sentiment_txt_to_db(words, level)

def users_data_txt_file_read(filename):
    f = open(filename)
    lines = f.readlines()
    for l in lines:
        words = l.split("\t") 
        if len(words) != 4:
            print words
            print len(words)
        dbutil.users_data_txt_to_db(words)

def checkin_txt_file_read(filename):
    f = open(filename)
    lines = f.readlines()
    for l in lines:
        words = l.split("\t") 
        if len(words) != 7:
            print words
            print len(words)
        dbutil.checkin_txt_to_db(words)

if __name__ == "__main__":
    print "txt_to_sql"
