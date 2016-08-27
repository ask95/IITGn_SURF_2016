from random import randint
import random
from missing_data import *
from operator import itemgetter


const = 2
delay = 10
N = 4000
num_text = 4
fract_correct = 0.75
fract_not_correct = 0.25
num_correct = int(fract_correct * N)
num_not_correct = int(fract_not_correct * N)
num_error = int(num_not_correct / 2)
num_no_resp = int(num_not_correct / 2)
N = num_no_resp+num_error + num_correct
num_slices = 15


# time,id,text
# time[randint(0,m-1) for p in range(0,num)]

time_list = range(0, N)
id_list = [1000+p for p in time_list]
text_list = [str(randint(0, num_text)) for p in time_list]
input_list = zip(*[time_list, id_list, text_list])


input_messg = [Message(p[1], p[0], p[2]) for p in input_list]
time_list_out = [randint(p, p+delay) for p in time_list]
id_list_out = id_list
text_list_out = text_list
check_ID = []
for i in range(len(time_list_out)):
    a = randint(0, 3)
    if a == 0:
        time_list_out[i] = time_list_out[i]+(delay*const)
    else:
        check_ID.append(id_list[i])
        continue


list_1_sorted = zip(*[time_list_out, id_list_out, text_list_out])
output_list = sorted(list_1_sorted, key=itemgetter(0))
resp_tuple = [Message(p[1], p[0], p[2]) for p in output_list]
# Input_stream=Stream('test_stream','Testing',[],64,16)
reqStream = Stream('req')
respStream = Stream('resp')
outStream1 = Stream('timely')
outStream2 = Stream('untimely')
inputStream = [reqStream, respStream]
outStream = [outStream1, outStream2]
x = MissingData(reqStream, respStream, outStream1, outStream2, delay)

i = 0
j = 0
k = 8
for p in range(N / 10):
    reqStream.extend(input_messg[i:10+i])
    i = i + 10
    respStream.extend(resp_tuple[j:j+k])
    j = j + k

respStream.extend(resp_tuple[j:])
extracted_list = outStream1.recent[:outStream1.stop]


for i in extracted_list:
    if i.ID in check_ID:
        continue
    else:
        print 'Error...'
        break
