from Stream import *
from Agent import *

def spacesaving_transition(no_of_pairs):
    def ss_transition(input_tuple, dic):
        start_index = input_tuple[0].start
        stop_index = input_tuple[0].stop
        input_lst = input_tuple[0].list[start_index:stop_index]

        for i in input_lst:
            if i in dic:
                dic[i] += 1

            elif len(dic) < no_of_pairs:
                dic.update({i: 1})

            else:
                min_key = min(dic, key=dic.get)
                min_val = dic[min_key]
                del dic[min_key]
                dic.update({i: min_val+1})

        start_index += len(input_lst)
        return [], dic, [start_index]
    return ss_transition

class SpaceSaving():
    def __init__(self,input_stream,epsil):
        self.stream = input_stream
        self.epsil = epsil
        self.num_pairs = int(1/epsil)
        self.structure = {}
        self.update = spacesaving_transition(self.num_pairs)
        self.countAgent = Agent([self.stream], \
                                [], self.update, self.structure)

    def query(self):
        return self.structure
        
        

        
            

