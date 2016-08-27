from Stream import *
from Operators import *
from abstract_database import *
import sqlite3
import datetime
import time
import sys
from random import *
#from stream_database import input_format


N = 400;
time_lst = range(0,N)
ip_list = ['69.89.31.226', '69.89.31.113', '69.89.31.200', '69.89.31.100']
blk_list = ['www.blacklisted0.com', 'www.blacklisted1.com', 'www.blacklisted2.com','www.blacklisted3.com',
            'www.blacklisted4.com', 'www.blacklisted5.com', 'www.blacklisted6.com', 'www.blacklisted7.com']
wht_list = 'www.google.com'
date = ['16-06-2016']*N


ip = []
for i in time_lst:
    a = randint(0, 3)
    ip.append(ip_list[a])

check_ip = []
check_url = []
check_tm = []
url_list = []
for i in time_lst:
    a = randint(0, 3)
    if (a == 0):
        url_list.append(wht_list)

    else:
        a = randint(0,len(blk_list)-1)
        url_list.append(blk_list[a])
        check_url.append(blk_list[a])
        check_ip.append(ip[i])
        check_tm.append(i)

input_stream = zip(ip, url_list, date, time_lst)
                         


    
    

        
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





def func(data_file, state_func):
    conn = sqlite3.connect('blacklisted_monitor.db')
    c = conn.cursor()

    sql_command = "SELECT date_stream, time_stream, IP_stream, url_stream FROM dataStream, blacklisted WHERE url_stream = URL"
    c.execute(sql_command)

    output_lst = c.fetchall()
    c.executemany("INSERT INTO blacklisted_access VALUES (?,?,?,?)", output_lst)
    conn.commit()
    conn.close()
    return [], None

## def date_time():
##    date1 = str(datetime.datetime.date(datetime.datetime.now()))
##    time1 = time.time()
##    return [date1, time1]
##
##
##
                         
                         
db_file = 'blacklisted_monitor.db'
stream_scheme = ['IP_stream text', 'url_stream text', 'date_stream text', 'time_stream real']
inp_Stream = Stream('input')

test_obj = databaseStream(inp_Stream, stream_scheme, func, 'blacklisted_monitor.db', 100, 100, [], None)
i = 0
for p in range(N / 10):
    inp_Stream.extend(input_stream[i:10+i])
    i = i+10

num = []

for i in blk_list:
    a=check_url.count(i)
    num.append(a)
num.extend([0,0])

lst = []

conn1 = sqlite3.connect('blacklisted_monitor.db')
c1 = conn1.cursor()

for i in range(10):
    url_name = "www.blacklisted" + str(i) + ".com"
    command = "SELECT COUNT(*) FROM blacklisted_access WHERE url_access = " + r"'" + url_name + r"'"
    c1.execute(command)
    lst.append(c1.fetchall())

print lst
print num
    
conn1.close()                         






    
        
    



