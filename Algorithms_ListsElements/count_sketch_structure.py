from Stream import *
from Agent import *
import numpy as np
import hashlib


'''
def query(queried_element, c_struct):
    lst_of_freqs = []
    for j in range(number_of_arrays):
        ind =  my_hash_value(queried_element, j, each_array_size)
        lst_of_freqs.append(c_struct[j][ind])

    return min(lst_of_freqs)
'''
def sign_hash(num, array_NUM):
    val = num + array_NUM

    if (val%2 == 0):
        return 1
    else:
        return -1

def update(ip_lst, c_struct):
    def my_hash_value(input_element, array_no, size_of_array):
        m = hashlib.md5()
        m.update(str(input_element) + str(array_no))
        hash_hex = int(m.hexdigest()[-8:],16)
        return (hash_hex)%(size_of_array)
    
    inlist_tuple=ip_lst[0]
    startindex=inlist_tuple.start
    stopindex=inlist_tuple.stop
    lst=inlist_tuple.list
    lst=lst[startindex:stopindex]
    for i in lst:
        for j in range(c_struct.shape[0]):
            ind = my_hash_value(i, j, c_struct.shape[1])
            c_struct[j][ind] += sign_hash(i,j)*1
    startindex=startindex+len(lst)
    return [],c_struct,[startindex]

class Count_Min_Sketch(object):
    def __init__(self, input_stream, no_of_arrays, size_of_array):
        self.input_stream = input_stream
        self.count_struct = np.zeros([no_of_arrays,size_of_array],'float')
        self.countAgent = Agent([self.input_stream], [], update, self.count_struct)

    def query(self, queried_element):
        def my_hash_value(input_element, array_no, size_of_array):
            m = hashlib.md5()
            m.update(str(input_element) + str(array_no))
            hash_hex = int(m.hexdigest()[-8:],16)
            return (hash_hex)%(size_of_array)
        lst_of_freqs = []
        nu_rows = self.count_struct.shape[0] 
        for j in range(nu_rows):
            ind = my_hash_value(queried_element, j, self.count_struct.shape[1])
            lst_of_freqs.append(self.count_struct[j][ind])
        lst_of_freqs.sort()
        med_ind = (len(lst_of_freqs))/2
        return lst_of_freqs[med_ind]



    
        

    

    

    
    
