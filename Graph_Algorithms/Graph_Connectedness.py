from Stream import *
from Agent import *
from collections import namedtuple
import copy

edge = namedtuple('Edge',['start_vertice','stop_vertice'])

class Edge():
  def __init__(self,startv, stopv):
    self.start_vertice = startv
    self.stop_vertice = stopv
'''
def find_cycle(NODES, EDGES): #, READY):
        print NODES
	todo = list(NODES)
	while todo:
		node = todo.pop()
		stack = [node]
		while stack:
			top = stack[-1]
      		#print EDGES[top]
      		for node in EDGES[top]:
        		if node in stack:
          			print 1
          			return 1 #stack[stack.index(node):]
        		if node in todo:
          			stack.append(node)
          			todo.remove(node)
          			break
      			else:
        			node = stack.pop()
        			#READY(node)
  	print 0
  	return 0 #None
'''

def find_cycle(NODES,EDGES):
    print NODES
    todo = list(NODES)
    while todo:
        node = todo.pop()
        stack = [node]
        while stack:
            top = stack[-1]
            for node in EDGES[top]:
                if node in stack:
                    #print 1
                    return 1
                if node in todo:
                    stack.append(node)
                    todo.remove(node)
                    break
            else:
                node = stack.pop()

    #print 0
    return 0


                    
def update(input_tuple, dic):
    #print input_tuple
    start_index = input_tuple[0].start
    stop_index = input_tuple[0].stop
    input_lst = input_tuple[0].list[start_index:stop_index]
    modified_dic = {}
    dic1 = {}

    for i in input_lst:
        #print i
        #create data structure that includes this edge.
        start_v = i.start_vertice
        stop_v = i.stop_vertice
        modified_dic = {}
        dic1 = {}
        
        if start_v not in dic:
            #print "Hi"
            dic.update({start_v:[stop_v]})
            modified_dic = copy.deepcopy(dic)
        else:
            if stop_v not in dic[start_v]:
                #print "Hi"
                #print dic
                modified_dic = copy.deepcopy(dic)
                modified_dic[start_v].append(stop_v)
                #print dic

        if stop_v not in dic:
            #print "Hi"
            dic.update({stop_v:[]})
            modified_dic.update({stop_v:[]})

        #print "Modified dic is ", modified_dic
                
        no_of_vertices = len(modified_dic)

        cycles_absent = not (find_cycle(modified_dic.keys(), modified_dic))
        #print dic
        print cycles_absent
        if cycles_absent:
            dic1 = copy.deepcopy(modified_dic)
        else:
            dic1 = dic

        #print "dic1 is ", dic1
        

    start_index += len(input_lst)
    print dic1
    return [], dic1, [start_index]


class findConnectivity():
    def __init__(self,input_stream):
        self.stream = input_stream
        #self.structure = {}
        self.update = update
        self.graphAgent = Agent([self.stream], \
                                [], self.update,self.structure)

        
        

    def query(self):
        no_of_edges = 0
        struct = self.graphAgent.state
        for i in struct:
            print len(struct[i])
            no_of_edges += len(struct[i])

        if (no_of_edges == len(struct)-1):
            return "CONNECTED"
        else:
            return "NOT CONNECTED"

        

    

            
                
            
        

