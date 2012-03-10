def cvt_checkin_data(fni):
    fi = open(fni)
    fo = open(fni+"_cleaned", 'w')
    print "open the file"
    while 1:
        lines = fi.readlines(1000)
        #print lines
        if not lines:
            break
        else:
            for line in lines:
                line = line.rstrip("\n")
                words = line.split("\t")
                #if len(words) != 8:
                    #print words
                #print words
                txt = ''
                #for word in words:
                #    txt += word + "\t"
                # if count placeid, it should be range of 7
                for i in range(0, 6):
                    #print i
                    #if (i < len(words)-1):
                    #    txt += words[i]
                    #txt += "\ta"
                    if words[i] and words[i] != "\n":
                        #print words
                        txt += words[i]
                    #else:
                    #    print words
                    #    print words[i]+"**"
                    txt += "\t"
                txt += "\n"
                fo.write(txt)
                #print txt
                #print txt.split("\t")
                #if len(txt.split("\t")) != 6:
                #    print txt.split("\t")
                #print txt.split("\t")
        #break
    fi.close()
    fo.close()


def cvt_sent_world(fni):
    fi = open(fni)
    fo = open(fni+"_cleaned", 'w')
    print "open the file"
    ln = 0
    while 1:
        lines = fi.readlines(1000)
        #print lines
        if not lines:
            break
        else:
            for line in lines:
                ln = ln + 1
                line = line.rstrip("\n")
                words = line.split("\t")
                #if ln == 7508 or ln == 7509 or ln == 7510:
                #    print words, ln
                #print len(words)
                #if len(words) != 3:
                #    print words
                #print words
                txt = ''
                #for word in words:
                #    txt += word + "\t"
                # if count placeid, it should be range of 7
                #for i in range(0, 3):
                    #print i
                    #if (i < len(words)-1):
                    #    txt += words[i]
                    #txt += "\ta"
                    #if words[i] and words[i] != "\n":
                        #print words
                        #txt += words[i]
                    #if words[i] == "27090185351":
                    #    break
                    #else:
                    #    print words
                    #    print words[i]+"**"
                    #txt += "\t"
                txt += words[0]+"\t"
                txt += words[2]+"\t"
#break
                txt += "\n"
                fo.write(txt)
                #print txt
                #print txt.split("\t")
                #if len(txt.split("\t")) != 6:
                #    print txt.split("\t")
                #print txt.split("\t")
        #break
    fi.close()
    fo.close()


'''
    while True:
        lines = f.readlines(10000)
        #print "read 1000 lines"
        if not lines:
            break
        for l in lines:
            words = l.split("\t")
            if len(words) != 7:
                print len(words), words
'''

def cvt_file(filename):
    f = open(filename)
    while 1:
        cvt(f)

if __name__ == "__main__":
    print "hello"
    #cvt_checkin_data("./checkin_data.txt")
    #cvt_sent_world("./sent_world.txt")
    cvt_sent_world("./sent_uk.txt")
    #cvt("./users_data.txt")
    print "end"
