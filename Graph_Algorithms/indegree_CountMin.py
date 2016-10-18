'''
This module is a streaming algorithm developed to compute the (in)degree centrality of vertices using CountMin sketch.
CountMin provides approximate frequencies for each distinct element in the input stream. Accuracy of the approximation based on the dimensions of the 2D array used to store these frequencies. Exact value of the gurantee is derived in the CountMin paper.
Indegree centrality can be seen as the frequency of the destination vertex. In the case of undirected graphs, the edge aslso increases the source vertex frequency. In this way, CountMin is used for finite space streaming algorithm for computation of indegree centrality of the vertices, which otherwise would have required maintaining an entire graph structure.
'''

from streampy_classes import Stream
from streampy_classes import Agent
from streampy_classes import Operators
import networkx as nx
import numpy as np
import hashlib

'''
my_hash_value is the hash function used. This makes use of md5 hash library, which gives less collisions than the hashing done by a python dictionary
outer_update updates the 2D array. As discussed in Count-Min algorithm, multiple copies of the array is kept so as to get a better guarantee on the aaproximate frequency provided.
agent_update is the function that is fed to the corresponding agent eventually.
'''

def my_hash_value(input_element, array_no, size_of_array):
    m = hashlib.md5()
    m.update(str(input_element) + str(array_no))
    hash_hex = int(m.hexdigest()[-8:],16)
    return (hash_hex)%(size_of_array)

def outer_update(directedness):
    def agent_update(ip_lst, c_struct):
        lst=ip_lst[0]
        for i in lst:
            source = i[0]
            sink = i[1]
            for j in range(c_struct.shape[0]):
                ind_sink = my_hash_value(sink, j, c_struct.shape[1])
                c_struct[j][ind_sink] += 1
                if not directedness:
                    ind_source = my_hash_value(source, j, c_struct.shape[1])
                    c_struct[j][ind_source] += 1
        return [], c_struct
    return agent_update


class indegree_CountMin(object):

    '''
    is_directed is the boolean for directedness of graph
    iStream is the input stream of edges
    count_structure is the 2D array that maintains the frequencies
    no_array being number of arrays and size_array being size of each array
    
    '''

    def __init__(self, iStream, is_directed, no_arrays, size_array, w_s = 15, step_size = 15, oStream= []):
        self.input_stream = iStream
        self.count_struct = np.zeros([no_arrays, size_array], 'float')
        self.is_directed = is_directed
        self.window_size = w_s
        self.step_size = step_size
        update = outer_update(self.is_directed)
        self.count_Agent = Operators.window_agent(update, [self.input_stream], [oStream], self.count_struct, None, self.window_size, self.step_size)

    def query(self, queried_vertice):
        lst_of_freqs = []
        nu_rows = self.count_struct.shape[0] 
        for j in range(nu_rows):
            ind = my_hash_value(queried_vertice, j, self.count_struct.shape[1])
            lst_of_freqs.append(self.count_struct[j][ind])
        return min(lst_of_freqs)




        

    

    

    
    
