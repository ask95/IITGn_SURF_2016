from Stream import *
from Operators import *
from examples_element_wrapper import print_stream
import numpy as np
from collections import namedtuple


output_tuple = namedtuple('output_tuple', ['ID', 'timetaken'])
input_tuple = namedtuple('input_tuple', ['time', 'id', 'text'])


class Message():
    def __init__(self, id, timestamp, content, category):
        self.id = id
        self.timestamp = timestamp
        self.content = content
        self.category = category
# dict key is ID of transaction/requester
# dictinary val is of the form [time_request, time_response, check]


def transition(max_time_taken):
    def trans(input_streams, state_list):
        start_req = input_streams[0].start
        start_resp = input_streams[1].start
        stop_req = input_streams[0].stop
        stop_resp = input_streams[1].stop
        request_stream = input_streams[0].list[start_req:stop_req]
        response_stream = input_streams[1].list[start_resp:stop_resp]
        timely_stream = []
        untimely_stream = []
        state_dict = state_list[0]
        final_timestamp = state_list[1]
        for i in request_stream:
            Id = i.id
            req_time = i.timestamp
            state_dict.update({Id: req_time})
        if len(response_stream) > 0:
            final_timestamp = response_stream[-1].timestamp
        for j in response_stream:
            ID = j.id
            if ID in state_dict:
                time_taken = j.timestamp - state_dict[ID]
                if time_taken > max_time_taken:
                    untimely_stream.append(output_tuple(ID, time_taken))
                else:
                    timely_stream.append(output_tuple(ID, time_taken))
                del state_dict[ID]
        keys_lst = state_dict.keys()
        for k in keys_lst:
            time_req = state_dict[k]
            if time_req + max_time_taken < final_timestamp:
                untimely_stream.append(output_tuple(k, time_req +
                                                    max_time_taken))
                del state_dict[k]
        state_list[1] = final_timestamp
        start_req += len(request_stream)
        start_resp += len(response_stream)
        assert (final_timestamp >= 0)
        if (len(request_stream) > 0):
            assert (request_stream[-1].timestamp >= final_timestamp)
        return [timely_stream, untimely_stream], state_list, [start_req,
                                                              start_resp]
    return trans


class MissingData:
    def __init__(self, reqStream, respStream, timely_stream, untimely_stream,
                 max_time_taken):
        assert (max_time_taken > 0)
        self.reqStream = reqStream
        self.respStream = respStream
        self.timely_stream = timely_stream
        self.untimely_stream = untimely_stream
        self.func = transition(max_time_taken)
        self.structure = [{}, 0]
        self._Agent = Agent([self.reqStream, self.respStream],
                            [self.timely_stream, self.untimely_stream],
                            self.func, self.structure)
