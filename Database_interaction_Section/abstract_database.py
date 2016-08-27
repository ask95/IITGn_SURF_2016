from Stream import *
from Agent import *
from Operators import window_agent
import sqlite3

#ip_stream_schema is list containing the name of attribute and its type
#stream is a stream of ordered but not named tuples
#state can be used for any extra arguments the user desires, state will be a list containing these arguments
#database used is to be decided by user. It is designed for a database which may contain othr relations that can now be used
##along with the stream window
#function to be written by the user as per what he requires to do and output


def dbTrans1(ip_stream_schema, func, database_file):
    def dbTrans2(ip_stream_list, state1 = None):
        conn = sqlite3.connect(database_file)
        c = conn.cursor()
        try:
            c.execute('''DROP TABLE dataStream''')
        except:
            pass
        
        create_str = 'CREATE TABLE dataStream ('
        for i in range(len(ip_stream_schema)):
            create_str += ip_stream_schema[i]
            if i == len(ip_stream_schema) - 1:
                create_str += ')'
            else:
                create_str += ', '

        print create_str
        c.execute(create_str)

        ip_stream = ip_stream_list[0]

        for i in ip_stream:
            insert_str = "INSERT INTO dataStream VALUES " + str(i)
            print insert_str
            c.execute(insert_str)
            conn.commit()

        conn.close()

        return func(database_file, state1)
    return dbTrans2


class databaseStream():
    def __init__(self, input_stream, stream_schema, uFunc, uDatabase, window_size = 50, step_size = 50, oStream = [], s = None):
        self.iStream = input_stream
        self.oStream = oStream
        self.stream_schema = stream_schema
        self.window_size = window_size
        self.step_size = step_size
        self.uState = s
        self.uFunc = uFunc
        self.uDatabase = uDatabase
        a = dbTrans1(stream_schema, self.uFunc, self.uDatabase)
        self.dbAgent = window_agent(a, [self.iStream], [self.oStream], self.uState, None, self.window_size, self.step_size)



        
