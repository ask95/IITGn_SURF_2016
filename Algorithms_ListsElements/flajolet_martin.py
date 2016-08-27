import hashlib
from Operators import *
from Agent import *
from Stream import *

#Let domain size of the stream elements be n
#in this case n = 500

'''
For this algo of course, num will be set to 2 when
this function is called. 
'''
def highestpowerofnum(element,num= 2):
    power = 0
    while (element%num == 0):
        power += 1
        element = element/num
    print 'accessed'

    return power

'''
n here is size of the output set of the hash function
In the algo, the size of the domain and the range set
of the hash function is equal to n
'''
def hash_val(element,n):
    m = hashlib.md5()
    m.update(str(element))
    hash_hex = int(m.hexdigest()[-8:],16)
    hash_value = hash_hex%n
    return hash_value


#input_list = []
#trail_zeros = 0

##def flajolet_martin(n):
##    def flaj_mart(lst_tuple, trail_zeros):
##        inlist_tuple=lst_tuple[0]
##        startindex=inlist_tuple.start
##        stopindex=inlist_tuple.stop
##        lst=inlist_tuple.list
##        lst=lst[startindex:stopindex]
##        for j in lst:
##            hash_value = hash_val(j,n)
##            no_of_zeros = highestpowerofnum(hash_value)
##            trail_zeros = max(no_of_zeros,trail_zeros)
##        startindex=startindex+len(lst)   
##        return [], trail_zeros, [startindex]
##    return flaj_mart

class Flajolet_Martin():
    
    def __init__(self,input_stream,size_of_domain_set):
        self.input_stream = input_stream
        self.n = size_of_domain_set
        self.trail_zeros = 0
        self.flajolet_martin_fn = self.flajolet_martin(self.n)
        self.agent= Agent([self.input_stream], [], self.flajolet_martin_fn,self.trail_zeros)
    def flajolet_martin(self,n):
        def flaj_mart(lst_tuple, zeros1):
            
##            def hash_val(element,n):
##                m = hashlib.md5()
##                m.update(str(element))
##                hash_hex = int(m.hexdigest()[-8:],16)
##                hash_value = hash_hex%n
##            return hash_value
            trail_zeros = zeros1
            inlist_tuple=lst_tuple[0]
            startindex=inlist_tuple.start
            stopindex=inlist_tuple.stop
            lst=inlist_tuple.list
            lst=lst[startindex:stopindex]
            for j in lst:
                hash_value = hash_val(j,n)
                no_of_zeros = highestpowerofnum(hash_value)
                trail_zeros = max(no_of_zeros,trail_zeros)
                print trail_zeros
            startindex=startindex+len(lst)
            self.trail_zeros=2**(trail_zeros+0.5)
            return [], trail_zeros,[startindex]
        return flaj_mart

