from random import randint
import random
from missing_data_multiplestreams import *
from operator import itemgetter

const = 2
catog = ['Gold', 'Platinum', 'Silver']
delay = {'Gold': 2, 'Platinum': 5, 'Silver': 10}
frac_gold = 0.2
frac_platinum = 0.3
frac_silver = 0.5
N = 4000
fract_correct = 0.75
fract_not_correct = 0.25
num_correct = int(fract_correct*N)
num_not_correct = int(fract_not_correct*N)
N = num_not_correct+num_correct
num_text = 4


time_list = range(0, N)
id_list = [1000+p for p in time_list]
text_list = [str(randint(0, num_text)) for p in time_list]
catog_list = [catog[randint(0, len(catog)-1)] for p in time_list]
input_list = zip(*[time_list, id_list, text_list, catog_list])
main_messg_req = [Message(p[1], p[0], p[2], p[3]) for p in input_list]
time_list_out = [randint(p, p+delay[catog_list[p]]) for p in
                 range(len(time_list))]
id_list_out = id_list
text_list_out = text_list
catog_list_out = catog_list

check_ID = {'Gold': [], 'Platinum': [], 'Silver': []}

for i in range(len(time_list_out)):
    a = randint(0, 3)
    if a == 0:
        time_list_out[i] = time_list_out[i]+(delay[catog_list_out[i]]*const)
    else:
        check_ID[catog_list_out[i]].append(id_list[i])
        continue
output_list = zip(*[time_list_out, id_list_out, text_list_out, catog_list_out])
output_list = sorted(output_list, key=itemgetter(0))

main_messg_resp = [Message(p[1], p[0], p[2], p[3]) for p in output_list]

reqStream = Stream('req')
respStream = Stream('resp')
x = MissingData_multistream(reqStream, respStream, catog, delay)

i = 0
j = 0
k = 8
for p in range(N / 10):
    reqStream.extend(main_messg_req[i:10+i])
    i = i+10
    respStream.extend(main_messg_resp[j:j+k])
    j = j+k

respStream.extend(main_messg_resp[j:])


list1 = check_ID['Gold']
list2 = check_ID['Platinum']
list3 = check_ID['Silver']
# #extracted_list=outStream1.recent[:outStream1.stop]

for i in delay:
    extracted_list = x.struct_dic[i][2].recent[:x.struct_dic[i][2].stop]
    check = check_ID[i]
    for j in extracted_list:
        if j.ID in check:
            print 'T'
            continue
        else:
            print 'Error...'
            break
