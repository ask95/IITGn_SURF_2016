from Stream import *
from Operators import *
from abstract_database import *
import sqlite3
import datetime
import time
import sys
#from stream_database import input_format


conn1 = sqlite3.connect('blacklisted_monitor.db')
c1 = conn1.cursor()

try:
    c1.execute('''DROP TABLE blacklisted''')
    c1.execute('''DROP TABLE blacklisted_access''')
except:
    pass

c1.execute('''CREATE TABLE blacklisted
             (URL text)''')
c1.execute('''CREATE TABLE blacklisted_access
             (date_access real, time_access real, IP_access text, url_access text)''')
for i in range(10):
    url_name = "www.blacklisted" + str(i) + ".com"
    command = "INSERT INTO blacklisted VALUES (" + r"'" + url_name + r"'" + ")"
    #print command
    c1.execute(command)
    conn1.commit()
c1.execute("SELECT * FROM blacklisted")
#print c.fetchall()
conn1.close()

db_file = 'blacklisted_monitor.db'

def func1(data_file, state_func):
    conn = sqlite3.connect('blacklisted_monitor.db')
    c = conn.cursor()

    sql_command = "SELECT date_stream, time_stream, IP_stream, url_stream FROM dataStream, blacklisted WHERE url_stream = URL"
    c.execute(sql_command)

    output_lst = c.fetchall()
    c.executemany("INSERT INTO blacklisted_access VALUES (?,?,?,?)", output_lst)
    conn.commit()
    return [], None

def date_time():
    date1 = str(datetime.datetime.date(datetime.datetime.now()))
    time1 = time.time()
    return [date1, time1]



stream_scheme = ['IP_stream text', 'url_stream text', 'date_stream text', 'time_stream real']
x = Stream('input')

test_obj = databaseStream(x, stream_scheme, func1, 'blacklisted_monitor.db', 2, 2, [], None)






    
        
    



