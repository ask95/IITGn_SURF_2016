from streampy_classes import Stream
from streampy_classes import Agent
from streampy_classes import Operators
from indegree_TOPK_MG import *
import networkx as nx

def test_MG_Class():
    input_stream = Stream.Stream('input')
    a = indegree_TOPK_MG(input_stream, 3, 1, 4, 4)
    input_stream.extend([(3,1),(4,1)])
    test_dict1 = a.topK_query()
    assert len(test_dict1) == 0
    input_stream.extend([(7,2),(2,1)])
    test_dict1 = a.topK_query()
    assert 1 in test_dict1
    input_stream.extend([(6,2),(2,1),(1,2)])
    a.topK_query()
    assert 1 in test_dict1
    assert 2 in test_dict1
