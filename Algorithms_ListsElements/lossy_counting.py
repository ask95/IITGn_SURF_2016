from Stream import *
from collections import namedtuple
from Agent import *

#delta is maximum possible error
class freq_tuple(object):
    def __init__(self,freq,delta):
        self.freq=freq
        self.delta=delta
        

#freq_tuple = namedtuple('freq_tuple', ['freq','delta'])
#input_list= []
#d_struct = {}
'''
compl_struct is a list of 3 objects: a dictionary with key as element and value as (freq,delta) tuple

'''

def loss_count_transition(bucket_size):
    
    def lossy_counting_transition(inp_tuple, compl_struct):
        start_index = inp_tuple[0].start
        stop_index = inp_tuple[0].stop
        input_lst = inp_tuple[0].list[start_index:stop_index]
        
        d_struct = compl_struct[0]
        actual_loc = compl_struct[1]
        
        for i in range(len(input_lst)):
            b_curr = int((actual_loc+i)/bucket_size)+ 1
           
            if input_lst[i] in d_struct:
                d_struct[input_lst[i]].freq = d_struct[input_lst[i]].freq+1

            else:
                new_tuple = freq_tuple(1, b_curr-1)
                d_struct.update({input_lst[i]: new_tuple})

            num = actual_loc + (i + 1)

            if (num%bucket_size == 0):
                key_lst = d_struct.keys()
                for j in key_lst:
                    sum_j = d_struct[j].freq + d_struct[j].delta
                    if sum_j < b_curr or sum_j == b_curr:
                        del d_struct[j]

        compl_struct[1] += len(input_lst)
        start_index += len(input_lst)
        return [], compl_struct, [start_index]
    return lossy_counting_transition


class LossyCounting():
    def __init__(self, input_stream, epsil):
        self.stream = input_stream
        self.epsil = epsil
        self.bkt_width = int(1/epsil)
        self.counting_struct = [{},0]
        self.update = loss_count_transition(self.bkt_width)
        
        self.countAgent = Agent([self.stream], \
                                [], self.update, self.counting_struct)

    def query(self,support):
        output_lst = []
        N = self.counting_struct[1]
        dic1 = self.counting_struct[0]
        for i in dic1:
            f = dic1[i].freq
            threshold = (support - self.epsil)*N
            if f > threshold or f == threshold:
                output_lst.append(i)
        return output_lst

'''
lst1=[0, 7, 2, 2, 1, 5, 4, 0, 3, 1, 5, 3, 3, 8, 7, 8, 6, 2, 4, 6, 6, 1, 0, 1, 8, 7, 8, 1, 0, 8, 2, 8, 8, 8, 3, 7, 2, 0, 0, 3, 7, 1, 5, 0, 0, 4, 5, 5, 7, 2, 0, 2, 7]
x=Stream('test_stream','Testing',[],64,16)
y=LossyCounting(x,0.1)
x.extend(lst1)
y.query(0.5)
    
'''
    
    

        
    
        
