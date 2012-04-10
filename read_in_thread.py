import Queue
import threading
import time
import sqlite3
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


db_file_name = './resultt/t.db'

queue_in = Queue.Queue(100)
queue_out = Queue.Queue(100)

class ThreadRead(threading.Thread):
    def __init__(self, queue_in, queue_out, read_process):
        threading.Thread.__init__(self)
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.read_process = read_process
        #self.lock = Lock()
        
    def run(self):
        index = 0
        while True:
            line = self.queue_in.get()
            result = self.read_process(line)
            self.queue_out.put((index, result), True, None)
            self.queue_in.task_done()
            index = index +1
            print "read: "+str(index)
    
class ThreadWrite(threading.Thread):
    sql = '''
INSERT INTO checkin_data_one (t_id, u_id, latitude, longitude, create_time, place_id, text) VALUES (?, ?, ?, ?, ?, ?, ?)
'''
    def __init__(self, out_queue, write_process):
        threading.Thread.__init__(self)
        self.queue_out = queue_out
        self.write_process = write_process
        #self.conn = sqlite3.connect('./tt/t.db', check_same_thread=False)
        #self.c = self.conn.cursor()

    def run(self):
        while True:
            index, result = self.queue_out.get()
            self.write_process(result)
            #try:
            #    c = self.conn.cursor()
            #    c.executemany(self.sql, result)
            #    self.conn.commit()
            #    c.close()
            #except sqlite3.OperationalError, msg:
            #    self.queue_out.put((index, result), True, None)
            self.queue_out.task_done()
            print "write: "+str(index)

def file_read(lines):
    words_list = []
    for l in lines:
        words = l.split('\t')
        u_id = words[0]
        tweet_id = words[1]
        latitude = words[2]
        longitude = words[3]
        createdat = words[4]
        text = unicode(words[5], encoding='utf-8')
        place_id = unicode(words[6], encoding='utf-8').strip('\n')
        word = (tweet_id, u_id, latitude, longitude, createdat, text, place_id)
        words_list.append(word)
    return words_list

sql = '''
INSERT OR IGNORE INTO checkin_data_one (t_id, u_id, latitude, longitude, create_time, place_id, text) SELECT tweet_id, u_id, latitude, longitude, createdat, place_id, text FROM checkin_data
'''
def sqlite3_write(conn, words_list):
    c = conn.cursor()
    c.executemany(sql, words_list)
    conn.commit()
    c.close()

sql_init = '''
CREATE TABLE IF NOT EXISTS users_data_one (
  u_id INTEGER NOT NULl PRIMARY KEY,
  status_count INTEGER,
  followers_count INTEGER,
  friends_count INTEGER
);
CREATE TABLE IF NOT EXISTS checkin_data_one (
  t_id INTEGER NOT NULL PRIMARY KEY,
  u_id INTEGER, 
  latitude REAL, 
  longitude REAL, 
  create_time TEXT,
  place_id TEXT,  
  text TEXT, -- i may delete it later
  stm TEXT,
  lang TEXT
);
'''

def buck_insert(words_list):
    rs = []
    for words in words_list:
        tweet_id, u_id, latitude, longitude, createdat, text, place_id = words
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
        rs.append(checkin)
        #checkin.save()
    db.bulk_save(rs)
    


start = time.time()
def main():
    #conn = sqlite3.connect('./tt/t.db', check_same_thread=False)
    #c = conn.cursor()
    #c.executescript(sql_init)
    #conn.commit()
    #c.close()

    for i in range(100):
        t = ThreadRead(queue_in, queue_out, file_read)
        t.setDaemon(True)
        t.start()
    
    for i in range(100):
        o = ThreadWrite(queue_out, buck_insert)
        o.setDaemon(True)
        o.start()
        
    queue_in.join()
    queue_out.join()

    f = open("./data/icwsm_2011/checkin_data.txt")
    index = 0
    while True:
        lines = f.readlines(10000)
        if not lines:
            break
        queue_in.put(lines, True, None)
    #for line in lines:
        #queue_in.put(line)
        index = index + 1 
        print "file: "+str(index)
    f.close()



main()
print "Elapsed Time: %s" % (time.time() - start)
