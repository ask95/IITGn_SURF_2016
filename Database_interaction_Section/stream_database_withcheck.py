from Stream import *
from Agent import *
import MySQLdb
import random
import datetime
import time
import sys

NUMBER_OF_STREAMS = 5

class input_format():
    def __init__(self, i_IP, i_url):
        self.IP = i_IP
        self.url = i_url
        self.time_of_access =  time.time()
        self.date = datetime.datetime.date(datetime.datetime.now())
        

#db = MySQLdb.connect("localhost","root","anikesh","cyberroam_iitgn")
#cursor = db.cursor()

def database_check(named_pipe, login, password, dbname):
    def db_transition(input_stream, state):
        list_of_streams = state[0]
        check_further = state[1]
        
        start_index = input_stream[0].start
        stop_index = input_stream[0].stop
        list_stream = input_stream[0].list[start_index:stop_index]
        print "Hi!"
        
        db = MySQLdb.connect(named_pipe, login, password, dbname)
        cursor = db.cursor()

        for i in list_stream:
            inp_url = i.url
            #query1 = "SELECT * from BLACKLISTED where URL = " + inp_url + r'"'
            #print query1
            cursor.execute("SELECT * FROM BLACKLISTED WHERE URL = %s", (inp_url,))
            db_lst = cursor.fetchall()
            if (len(db_lst) > 0):
                db_op = db_lst[0][0]
                print db_op
                print inp_url
                Result_id = random.randrange(10**7, 10**8)
                cursor.execute('''INSERT into BLACKLISTED_ACCESS (RIid, IP, url,ProcessDate, ProcessTime) values (%s, %s, %s, %s, %s)''',
                (Result_id, i.IP, i.url, i.date, i.time_of_access))
                db.commit()

                ip1 = random.randint(0, NUMBER_OF_STREAMS)
                ip2 = random.randint(0, NUMBER_OF_STREAMS)

                if check_further:
                    check_streams = []
                    for j in list_of_streams:
                        if j.name == ip1 or j.name == ip2:
                            check_streams.append(j)
                            
                    Result_id = random.randrange(10**7, 10**8)
                    cursor.execute('''INSERT into CHECKED_IP (CIid, MAIN_IP, FIRST_IP, SECOND_IP, ProcessDate, ProcessTime) values (%s, %s, %s, %s, %s)''',
                    (Result_id, i.IP, check_streams[0].name, check_streams[1].name, datetime.datetime.date(datetime.datetime.now()), time.time()))
                    db.commit()

                    for k in check_streams:
                        Stream_Data_Check(k, list_of_streams, 0)

        start_index += len(list_stream)
        return [], state, [start_index]
    return db_transition

class Stream_Database_Check():
    def __init__(self, i_stream, streams_list, bool1 = 1, i_named_pipe = "localhost", i_login = "root" \
                 , i_password = "anikesh", i_dbname = "cyberroam_iitgn"):
        self.stream_input = i_stream
        self.list_of_streams = streams_list
        self.trans = database_check(i_named_pipe, i_login, i_password, i_dbname)
        self.dbAgent = Agent([self.stream_input], [], self.trans, [self.list_of_streams, bool1])
