from couchbase.couchbaseclient import VBucketAwareCouchbaseClient as couchbaseClient
import uuid

server = {"ip": "127.0.0.1", "port": "8091",
    "rest_username": "admin", "rest_password": "horizon",
    "username": "admin", "password": "horizon"
    }

v = couchbaseClient(server, 'world')

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
            checkin = text
            #checkin.save()
            key = uuid.uuid4().bytes.encode('hex')
            setstatus = v.set(key, 0, 0, checkin)
        print index
        index = index + 1
    f.close()


file_read()
#
#for val in range(0,1000):
#  key = uuid.uuid4().bytes.encode('hex')
#  setstatus = v.set(key, 0, 0, str(val))
#print setstatus
v.done()
