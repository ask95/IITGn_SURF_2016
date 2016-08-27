from Stream import *
from Agent import *
import MySQLdb
import random
import datetime
import time
import sys

class input_format():
    def __init__(self, i_IP, i_url):
        self.IP = i_IP
        self.url = i_url
        self.time_of_access =  time.time()
        self.date = datetime.datetime.date(datetime.datetime.now())
        

#db = MySQLdb.connect("localhost","root","anikesh","cyberroam_iitgn")
#cursor = db.cursor()

def database_check(named_pipe, login, password, dbname):
    def db_transition(input_stream, state = None):
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
                if db_op == inp_url:
                    Result_id = random.randrange(10**7, 10**8)
                    cursor.execute('''INSERT into BLACKLISTED_ACCESS (RIid, IP, url,ProcessDate, ProcessTime) values (%s, %s, %s, %s, %s)''',
                    (Result_id, i.IP, i.url, i.date, i.time_of_access))
                    db.commit()

        start_index += len(list_stream)
        return [], None, [start_index]
    return db_transition

class Stream_Database_Check():
    def __init__(self, i_stream, i_named_pipe = "localhost", i_login = "root" \
                 , i_password = "anikesh", i_dbname = "cyberroam_iitgn"):
        self.stream_input = i_stream
        self.trans = database_check(i_named_pipe, i_login, i_password, i_dbname)
        self.dbAgent = Agent([self.stream_input], [], self.trans, None)
