from Queue import Queue
import time
from threading import Thread, Lock
import sqlite3

class FetchIO:
    def __init__(self, process):
        self.lock = Lock()
        self.q_req = Queue(10)
        self.q_ans = Queue(10)
        for i in range(10):
            t = Thread(target=self.threadget)
            t.setDaemon(True)
            t.start()
        self.running = 0
        self.process = process

    def __del__(self):
        self.q_rep.join()
        self.q_ans.join()
        
    def taskleft(self):
        return self.q_req.qsize()+self.q_ans.qsize()+self.running

    def push(self, req):
        self.q_req.put(req, True, None)
        print "push"
    def pop(self):
        self.q_ans.get()

    def threadget(self):
        while True:
            req = self.q_req.get()
            with self.lock:
                self.running += 1
            out = self.process(req)
            self.q_ans.put(out, True, None)
            with self.lock:
                self.running -= 1
            self.q_req.task_done()


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



def main():
    conn = sqlite3.connect('./resultt/thread.db')
    c = conn.cursor()
    c.executescript(sql_init)
    conn.commit()
    c.close()

    fetch = FetchIO(process=file_read)
    f = open("./data/icwsm_2011/checkin_data.txt")
    index = 0
    while True:
        lines = f.readlines(20)
        if not lines:
            break
        fetch.push(lines)
    #for line in lines:
        #queue_in.put(line)
        index = index + 1 
        print "file: "+str(index)
    while fetch.taskleft():
        index, words_list = fetch.pop()
        sqlite3_write(conn, words_list)
        print "write"
    f.close()
        

main()
    
