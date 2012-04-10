import datetime 
from couchdbkit import *

class CheckIn(Document):
    t_id = IntegerProperty()
    u_id = IntegerProperty()
    lat = StringProperty()
    lng = StringProperty()
    time = StringProperty()
    place = StringProperty()
    text = StringProperty()
    sentiment = StringProperty()
    language = StringProperty()

server = Server()
db = server.get_or_create_db("checkin")
CheckIn.set_db(db)


file_path = "./data/icwsm_2011/checkin_data.txt"

def file_read():
    index = 0
    f = open(file_path)
    while True:
        lines = f.readlines(10000)
        if not lines:
            break
        for line in lines:
            words = line.split('\t')
            u_id = words[0]
            tweet_id = words[1]
            latitude = words[2]
            longitude = words[3]
            createdat = words[4]
            text = unicode(words[5], encoding='utf-8')
            place_id = unicode(words[6], encoding='utf-8').strip('\n')
            checkin = CheckIn(
                t_id = int(tweet_id),
                u_id = int(u_id),
                lat = latitude,
                lng = longitude,
                time = createdat,
                place = place_id,
                text = text,
                sentiment = '',
                language = ''
                )
            checkin.save()
        print index
        index = index + 1
    f.close()


file_read()
