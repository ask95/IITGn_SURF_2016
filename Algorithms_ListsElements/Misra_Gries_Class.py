'''
Misra_Gries algorithm is a one pass streaming algorithm, that returns all the frequent
items in the given input stream (after one pass through each element). Frequent is defined
on the basis of a parameter k. If the length of the stream is n, then a frequent item is defined
as an element having frequency atleast (n/k). The size of the output dictionary returned has length k-1. 
For a particular itemset returned, its not necessary that every element in the set is frequent, 
however if there are any frequent items, it will definitely lie in the output dictionary with 100% probability.  
'''

from streampy_classes import Stream
from streampy_classes import Agent
from streampy_classes import Operators
#from streampy_classes import examples_element_wrapper
#from examples_element_wrapper import print_stream
#from Misra_Gries_list import Misra_Gries_list

'''
Initially Misra_Gries_Algo was used as the input function for the agent,
however we needed to provide the extra parameter k. Hence this fucntion was
modified to Misra_Gries_list, a function taking parameter K and returning 
the function to be given as input to the agent.
'''

#K is the parameter described in the algorithm
#key_freq is the dictionary that contains the item and its frequency,

#an important point is that the frequency is not necessarily accurate,
#but the frequent items are returned, maybe with some stray values.
 
def Misra_Gries_fn(K):
    k=K
    def Misra_Gries_Algo(lst1, key_freq):
        lst=lst1[0]
        for i in range(len(lst)):
            element = lst[i]    
            if key_freq.has_key(element):
                key_freq[element] += 1
            
            elif len(key_freq) < k:
                key_freq.update({lst[i]:1})
            
            else:
                keys_list = key_freq.keys()
                for j in range(len(keys_list)):
                    key_freq[keys_list[j]] -= 1
                    if key_freq[keys_list[j]] == 0:
                        del key_freq[keys_list[j]]
        #print key_freq
        return None , key_freq
    return Misra_Gries_Algo

class Misra_Gries_Class(object):
    def __init__(self, input_s, k, w_size = 15, step_size = 15, oStream = []):
        self.iStream = input_s
        self.oStream = oStream
        self.K = k
        self.window_size = w_size
        self.step_size = step_size
        mg_f = Misra_Gries_fn(self.K)
        self.topK = {}
        
        self.mg_Agent = Operators.window_agent(mg_f, [self.iStream], [self.oStream], self.topK, None, self.window_size, self.step_size)

    def topK_query(self):
        print self.topK
        return self.topK
    


'''
lst_inputs = [input_stream]
lst_call = [input_stream]

key_freq1 = {}
Misra_Gries_fn = Misra_Gries_list(3)
'''
'''
#as of now the call stream for the agent is the input itself

list_agent(Misra_Gries_fn, [input_stream], [], key_freq1, \
                   None, None, None)
'''

'''
We also tried using lf function, but it would give error on giving the extra parameter k
Hence we modified "Misra_Gries_Algo" to "Misra_Gries_list" function so that we can give the
'k' argument   
'''

#siz = 3

#def lf(inputs, outputs, func, state=None,
       #call_streams=None, **kwargs):
#lf([input_stream], [], Misra_Gries_fn, key_freq1, None)

input_stream = Stream.Stream('input')
a = Misra_Gries_Class(input_stream, 3)
input_stream.extend([1,2,3,2,1])
a.topK_query()
#print "Hey"
input_stream.extend([2,1,1,4,2])
#print "Input is:    "
#print_stream(input_stream)
input_stream.extend([1,2,2,4,3])
a.topK_query()
input_stream.extend([6,6,6,6,6])
a.topK_query()
input_stream.extend([6,6,6,6,6])
input_stream.extend([6,6,6,6,6])
a.topK_query()
