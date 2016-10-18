from streampy_classes import Stream
from streampy_classes import Agent
from streampy_classes import Operators
from indegree_CountMin import *
import networkx as nx

#from Misra_Gries_list import Misra_Gries_list


def test_MG_Class():
    input_stream = Stream.Stream('input')
    a = indegree_CountMin(input_stream, 1, 5, 10, 4, 4)
    input_stream.extend([(3,1),(4,1)])
    test1 = a.query(1)
    assert test1 == 0
    input_stream.extend([(7,2),(2,1)])
    test2 = a.query(1)
    assert int(test2) == 3
    input_stream.extend([(6,2), (1,2), (3,4), (6,7)])
    test3 = a.query(6)
    test4 = a.query(7)
    assert test3 == 0
    assert test4 == 1
