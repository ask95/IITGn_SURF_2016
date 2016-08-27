from Stream import *
from Operators import *
from examples_element_wrapper import print_stream
import numpy as np
from collections import namedtuple
from missing_data_multiple import *
from types import *


class Message():
    def __init__(self, id, timestamp, content, category):
        self.id = id
        self.timestamp = timestamp
        self.content = content
        self.category = category


def divide_with_op(output_dic):
    def divide_trans(input_streams, state=None):
        start_req = input_streams[0].start
        start_resp = input_streams[1].start
        stop_req = input_streams[0].stop
        stop_resp = input_streams[1].stop
        request_stream = input_streams[0].list[start_req:stop_req]
        response_stream = input_streams[1].list[start_resp:stop_resp]
        list_categries = {}
        keys = output_dic.keys()
        for p in keys:
            list_categries[p] = [[], []]
        assert (len(list_categries) == len(keys))
        for i in request_stream:
            cat = i.category
            list_categries[cat][0].append(i)
        for j in response_stream:
            cat1 = j.category
            list_categries[cat1][1].append(j)
        for p in list_categries.keys():
            output_dic[p][0].extend(list_categries[p][0])
            output_dic[p][1].extend(list_categries[p][1])
        start_req += len(request_stream)
        start_resp += len(response_stream)
        return [], None, [start_req, start_resp]
    return divide_trans


class MissingData_multistream():
    def __init__(self, main_req_stream, main_resp_stream, categories, delay):
        self.num_categories = len(categories)
        assert (self.num_categories > 0)
        self.main_req_stream = main_req_stream
        self.main_resp_stream = main_resp_stream
        dic = {}
        for i in categories:
            req_stream = Stream("Request_Category_" + str(i))
            resp_stream = Stream("Response_Category_" + str(i))
            timely_stream = Stream("Timely_Category_" + str(i))
            untimely_stream = Stream("Untimely_Category_" + str(i))
            dic.update({i: [req_stream, resp_stream, timely_stream,
                            untimely_stream,
                        MissingData(req_stream, resp_stream, timely_stream,
                                    untimely_stream, delay[i])]})
        self.struct_dic = dic
#        assert (self.struct_dic.keys()==categories)
#        assert (len(dic[categories[0]])==5)
#        assert (len(self.struct_dic)==self.num_categories)
        self.func1 = divide_with_op(self.struct_dic)
        self.divideAgent = Agent([self.main_req_stream, self.main_resp_stream],
                                 [],
                                 self.func1, None)
