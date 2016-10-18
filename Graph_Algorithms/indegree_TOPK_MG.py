'''
Misra_Gries algorithm is a one pass streaming algorithm, that returns all the frequent
items in the given input stream (after one pass through each element). Frequent is defined
on the basis of a parameter k. If the length of the stream is n, then a frequent item is defined
as an element having frequency atleast (n/k). The size of the output dictionary returned has length k-1. 
For a particular itemset returned, its not necessary that every element in the set is frequent, 
however if there are any frequent items, it will definitely lie in the output dictionary with 100% probability. 

Indegree centrality can be seen as the frequency of the destination vertex. In the case of undirected graphs, the edge aslso increases the source vertex frequency. In this way, Misra-Gries is used for finite space streaming algorithm for computation of n/k frequency elements, which otherwise could have required maintaining an entire graph structure.
'''

from streampy_classes import Stream
from streampy_classes import Agent
from streampy_classes import Operators
#from examples_element_wrapper import print_stream
import networkx as nx

#from Misra_Gries_list import Misra_Gries_list

def edge_update(iK, iDict, iNode, iBool):
    if iDict.has_key(iNode):
        iDict[iNode] += 1
            
    elif len(iDict) < iK:
        iDict.update({iNode:1})
            
    else:
        iBool = 1



def Misra_Gries_fn(K, isDirected):
    '''
    node1 is the source node
    node2 is the sink node
    bool1 is the boolean corresponding to whether the node1 has place in the MG frequency dictionary
    bool2 ------------------------------------------- node2 ----------------------------------------
    whether a subtraction should occur is a product of bool1 and bool2
    IN a directed graph, only node2 will have increase in IDC because of the edge
    '''
    
    k=K
    def Misra_Gries_Algo(lst1, key_freq):
        lst=lst1[0]
        for i in range(len(lst)):
            element = lst[i]
            node1 = element[0]
            node2 = element[1]
            bool2 = 0
            bool1 = 0

            edge_update(k, key_freq, node2, bool2)
            
            '''
            If graph is undirected then the source node should also have increase in Indegree Centrality
            In such a case, only its MG frequency should be affected and others should not be subtracted
            '''
            
            if isDirected:
                bool1 = 1
            else:
                edge_update(k, key_freq, node1, bool1)
                
            freq_subtract_bool = bool1*bool2
            
            if freq_subtract_bool:
                keys_list = key_freq.keys()
                for j in range(len(keys_list)):
                    key_freq[keys_list[j]] -= 1
                    if key_freq[keys_list[j]] == 0:
                        del key_freq[keys_list[j]]
        #print key_freq
        return None , key_freq
    return Misra_Gries_Algo

class indegree_TOPK_MG(object):

    '''
    is_directed is the boolean for directedness of graph
    iStream is the input stream of edges
    k sets the fraction for the frequency of the vertice
    i.e. if vertice frequency > N/k, it will be outputted
    '''
    
    def __init__(self, input_s, k, is_directed, w_size = 15, step_size = 15, oStream = []):
        self.iStream = input_s
        self.oStream = oStream
        self.K = k
        self.is_directed = is_directed
        self.window_size = w_size
        self.step_size = step_size
        mg_f = Misra_Gries_fn(self.K, self.is_directed)
        self.topK = {}
        
        self.mg_Agent = Operators.window_agent(mg_f, [self.iStream], [self.oStream], self.topK, None, self.window_size, self.step_size)

    def topK_query(self):
        print self.topK
        return self.topK
    


'''
input_stream = Stream('input')
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
'''