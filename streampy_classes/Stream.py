""" This module contains the Stream class. The
Stream and Agent classes are the building blocks
of PythonStreams.
(12 October 2015. Mani. Fixed bug. Made _no_value
and _close classes rather than object.)
"""

from SystemParameters import DEFAULT_STREAM_SIZE,\
                             DEFAULT_BUFFER_SIZE_FOR_STREAM
# Import numpy and pandas if StreamArray (numpy) and StreamSeries (Pandas)
# are used.
import numpy as np
import pandas as pd
from collections import namedtuple

import logging.handlers

# TimeAndValue is used for timed messages.
TimeAndValue = namedtuple('TimeAndValue', ['time', 'value'])

# _no_value is the message sent on a stream to indicate that no
# value is sent on the stream at that point. _no_value is used
# instead of None because you may want an agent to send a message
# with value None and for the agent receiving that message to
# take some specific action.
class _no_value(object):
    def __init__(self):
        pass

# _close is the message sent on a stream to indicate that the
# stream is closed.
class _close(object):
    def __init__(self):
        pass

# When _multivalue([x1, x2, x3,...]) is sent on a stream, the
# actual values sent are the messages x1, then x2, then x3,....
# as opposed to a single instance of the class _multivalue.
# See examples_element_wrapper for examples using _multivalue.
class _multivalue(object):
    def __init__(self, lst):
        self.lst = lst
        return

class _array(object):
    def __init__(self, value):
        self.value = value
        return
    
def remove_novalue_and_open_multivalue(l):
    """ This function returns a list which is the
    same as the input parameter l except that
    (1) _no_value elements in l are deleted and
    (2) each _multivalue element in l is opened
        i.e., for an object _multivalue(list_x)
        each element of list_x appears in the
        returned list.

    Parameter
    ---------
    l : list
        A list containing arbitrary elements
        including, possibly _no_value and
        _multi_value

    Returns : list
    -------
        Same as l with every _no_value object
        deleted and every _multivalue object
        opened up.

    Example
    -------
       l = [0, 1, _no_value, 10, _multivalue([20, 30])]
       The function returns:
           [0, 1, 10, 20, 30]

    """
    if not isinstance(l, list):
        return l
    return_list = []
    for v in l:
        if (isinstance(v, list) or
            isinstance(v, np.ndarray) or
            isinstance(v, tuple)):
            return_list.append(v)
        else:
            if v == _no_value:
                continue
            elif isinstance(v, _multivalue):
                return_list.extend(v.lst)
            else:
                return_list.append(v)
    return return_list


class Stream(object):
    """
    A stream is a sequence of values. Agents can:
    (1) Append values to the tail of stream and
    close a stream.
    (2) Read a stream.
    (3) Subscribe to be notified when a
    value is added to a stream.
    (See Agent.py for details of agents.)

    The ONLY way in which a stream can be
    modified is that values can be appended to its
    tail. The length of a stream (number of elements
    in its sequence) can stay the same or increase,
    but never decreases. If at some point, the length
    of a stream is k, then from that point onwards, the
    first k elements of the stream remain unchanged.

    A stream is written by only one agent. Any
    number of agents can read a stream, and any
    number of agents can subscribe to a stream.
    An agent can be a reader and a subscriber and
    a writer of the same stream. An agent may subscribe
    to a stream without reading the stream's values; for
    example the agent may subscribe to a clock stream
    and the agent executes a state transition when the
    the clock stream has a new value, regardless of
    the value.

    Parameters
    ----------
    name : str, optional
          name of the stream. Though the name is optional
          a named stream helps with debugging.
          default : 'NoName'
    proc_name : str, optional
          The name of the process in which this agent
          executes.
          default: 'UnknownProcess'
    initial_value : list or array, optional
          The list (or array) of initial values in the
          stream. If a stream starts in a known state, i.e.,
          with a known sequence of messages, then set the
          initial_value to this sequence.
          default : []
    stream_size: int, optional
          stream_size must be a positive integer.
          It is the largest number of the most recent
          elements in the stream that are in main memory.
          default : DEFAULT_STREAM_SIZE
                    where DEFAULT_STREAM_SIZE is
                    specified in SystemParameters.py
    buffer_size : int, optional
           buffer_size must be a positive integer.
           An exception may be thrown if an agent reads an
           element with index i in the stream where i is
           less than the length of the stream - buffer_size.
           default : DEFAULT_BUFFER_SIZE_FOR_STREAM
                     specified in SystemParameters.py

    Attributes
    ----------
    recent : list
          A list of the most recent values of the stream.
          recent is a NumPy array if specified.
    stop : int
          index into the list recent.
          s.recent[:s.stop] contains the s.stop most recent
          values of stream s.
          s.recent[s.stop:] contains padded values.
    offset: int
          index into the stream used to map the location of
          an element in the entire stream with the location
          of the same element in s.recent, which only
          contains the most recent elements of the stream.
          For a stream s:
                   s.recent[i] = s[i + s.offset]
                      for i in range(s.stop)
    start : dict of readers.
            key = reader
            value = start index of the reader
            Reader r can read the slice:
                      s.recent[s.start[r] : s.stop ]
            in s.recent which is equivalent to the following
            slice in the entire stream:
                    s[s.start[r]+s.offset: s.stop+s.offset]
    subscribers_set: set
             the set of subscribers for this stream.
             Subscribers are agents to be notified when an
             element is added to the stream.
    closed: boolean
             True if and only if the stream is closed.
             An exception is thrown if a value is appended to
             a closed stream.
    close_message: _close or np.NaN
            This message is appended to a stream to indicate that
            when this message is received the stream should be closed.
            If the stream is implemented as a list then close_message
            is _close, and for StreamArray the close_message is np.NaN
            (not a number).
    _buffer_size: int
            Invariant:
            For every reader r of stream s:
                 s.stop - s.start[r] < s._buffer_size
            A reader can only access _buffer_size number of
            consecutive, most recent, elements in the stream.
    _begin : int
            index into the list, recent
            recent[:_begin] is not being accessed by any reader;
            therefore recent[:_begin] can be deleted from main
            memory.
            Invariant:
                    for all readers r:
                          _begin <= min(start[r])

    Notes
    -----
    1. AGENTS SUBSCRIBING TO A STREAM

    An agent is a state-transition automaton and
    the only action that an agent executes is a
    state transition. If agent x is a subscriber
    to a stream s then x.next() --- a state
    transition of x --- is invoked whenever messages
    are appended to s.

    The only point at which an agent executes a
    state transition is when a stream to which
    the agent subscribes is modified.

    An agent x subscribes to a stream s by executing
            s.call(x).
    An agent x unsubscribes from a stream s by
    executing:
            s.delete_caller(x)


    2. AGENTS READING A STREAM

    2.1 Agent registers for reading

    An agent can read a stream only after it registers
    with the stream as a reader. An agents r registers
    with a stream s by executing:
                   s.reader(r)
    An agent r deletes its registration for reading s
    by executing:
                   s.delete_reader(r)

    2.2 Slice of a stream that can be read by an agent

    At any given point, an agent r that has registered
    to read a stream s can only read some of the most
    recent values in the stream. The number of values
    that an agent can read may vary from agent to agent.
    A reader r can only read a slice:
             s[s.start[r]+s.offset: s.stop+s.offset]
    of stream s where start[r], stop and offset are
    defined later.


    3. WRITING A STREAM

    3.1 Extending a stream

    When an agent is created it is passed a list
    of streams that it can write.

    An agent adds a single element v to a stream s
    by executing:
                  s.append(v)

    An agent adds the sequence of values in a list
    l to a stream s by executing:
                   s.extend(l)
    The operations append and extend of streams are
    analogous to operations with the same names on
    lists.

    3.2 Closing a Stream

    A stream is either closed or open.
    Initially a stream is open.
    An agent that writes a stream s can
    close s by executing:
                  s.close()
    A closed stream cannot be modified.

    4. MEMORY

    4.1 The most recent values of a stream

    The most recent elements of a stream are
    stored in main memory. In addition, the
    user can specify whether all or part of
    the stream is saved to a file.

    Associated with each stream s is a list (or
    array) s.recent that includes the most
    recent elements of s. If the value of s is a
    sequence:
                  s[0], ..., s[n-1],
    at a point in a computation then at that point,
    s.recent is a list
                    s[m], .., s[n-1]
    for some m, followed by some padding (usually
    a sequence of zeroes, as described later).

    The system ensures that all readers of stream
    s only read elements of s that are in s.recent.

    4.2 Slice of a stream that can be read

    Associated with a reader r of stream s is an
    integer s.start[r]. Reader r can only read
    the slice:
               s.recent[s.start[r] : ]
    of s.recent.

    For readers r1 and r2 of a stream s the values
    s.start[r1] and s.start[r2] may be different.

    4.3 When a reader finishes reading part of a stream

    Reader r informs stream s that it will only
    read values with indexes greater than or
    equal to j in the list, recent,  by executing
                  s.set_start(r, j)
    which causes s.start[r] to be set to j.


    5. OPERATION

    5.1 Memory structure

    Associated with a stream is:
    (1) a list, recent.
    (2) a nonnegative integer stop  where:
       (a) recent[ : stop] contains
           the most recent values of the stream,
       (b) the slice recent[stop:] is
           padded with padding values
           (either 0 or 0.0).
    (3) a nonnegative integer s.offset where
          recent[i] = stream[i + offset]
             for 0 <= i < s.stop

    Example: if the sequence of values in  a stream
    is:
               0, 1, .., 949
    and s.offset is 900, then
       s.recent[i] = s[900+i] for i in 0, 1, ..., 49.

    Invariant:
              len(s) = s.offset + s.stop
    where len(s) is the number of values in stream s.

    The size of s.recent is the parameter stream_size
    of s. Recommendations for the value of stream_size
    are given after a few paragraphs.

    The maximum size of the list that an agent can
    read is the parameter, buffer_size. Set
    buffer_size large enough so that the size of
    the slice that any agent wants to read is less
    than buffer_size. If an agent is slow compared to
    the rate at which the stream grows then the
    buffer_size should be large. For example, if
    an agent is reading the element in the stream
    at location i, and the stream has grown to l
    elements then buffer_size must be greater than
    l - i.

    (In later implementations, if an agent reads
    a part of stream s that is not in s.recent, then
    the value read is obtained from values saved to
    a file.)

    The entire stream, or the stream up to offset,
    can be saved in a file for later processing.
    You can also specify that no part of the stream
    is saved to a file. (Note, if the stream s is not
    saved, and any agent reads an element of the
    stream s that is not in main memory, then an
    exception is raised.)

    In the current implementation old values of the
    stream are not saved.

    5.2 Memory Management

    We illustrate memory management with the
    following example with stream_size=4 and
    buffer_size=1

    Assume that a point in time, for a stream s,
    the list of values in the stream is
    [1, 2, 3, 10, 20]; stream_size=4;
    s.offset=3; s.stop=2; and
    s.recent = [10, 20, 0, 0].
    The size s.recent is stream_size (i.e. 4).
    The s.stop (i.e. 2) most recent values in the
    stream are 10 followed by a later value, 20.
    s[3] == 10 == s.recent[0]
    s[4] == 20 == s.recent[1]
    The values  in s.recent[s.stop:] are padded
    values (zeroes).

    A reader r of stream s has access to the list:
      s.recent[s.start[r] : s.stop]
    So, if for a reader r, s.start[r] is 0,
    then r has access to the two most
    recent values in the stream, i.e.,
    the list [10, 20].
    If for another reader q, s.start[q]=1,
    then q has access to the list [20].
    And for another reader u, s.start[q]=2,
    then u has access to the empty list [].

    When a value v is appended to stream s, then
    v is inserted in s.recent, replacing a padded
    value, and s.stop is incremented. If the empty
    space (i.e., the number of padded values) in
    s.recent decreases below buffer_size then a
    new version of s.recent is created containing
    only the buffer_size most recent values of the
    stream.

    Example: Start with the same example as the previous
    example with buffer_size = 2

    Then a new value, 30 is appended to the stream,
    making the list of values in s:
    [1, 2, 3, 10, 20, 30]
    s.stop = 3; s.offset is unchanged (i.e. 3) and
    s.recent = [10, 20, 30, 0].
    Now the size of the empty space in s.recent is 1,
    which is less than buffer_size. So, the program sets
    s.recent to [20, 30, 0, 0], keeping the buffer_size
    (i.e. 2) most recent values in s.recent and removing
    older values from main memory, and it sets s.stop to
    buffer_size, and increments offset by
    s.stop - buffer_size. Now
       s.stop = 2
       s.offset = 4

    """
    def __init__(self, name="NoName", proc_name="UnknownProcess",
                 initial_value=[],
                 stream_size=DEFAULT_STREAM_SIZE,
                 buffer_size = DEFAULT_BUFFER_SIZE_FOR_STREAM):
        self._buffer_size = min(buffer_size, DEFAULT_STREAM_SIZE/4)
        self.name = name
        # Name of the process in which this stream lives.
        self.proc_name = proc_name
        # Create the list recent and the parameters
        # associated with garbage collecting
        # elements in the list.
        # Pad recent with any padded value (e.g. zeroes).
        self.recent = self._create_recent(stream_size)
        self._begin = 0
        # Initially, the stream is initial_value.
        # offset is 0.
        self.offset = 0
        self.stop = len(initial_value)
        self.recent[:self.stop] = initial_value
        # Initially the stream has no readers.
        self.start = dict()
        # Initially the stream has no subscribers.
        self.subscribers_set = set()
        # Initially the stream is open
        self.closed = False
        assert(isinstance(initial_value, list))
        # close_message is the message in the stream
        # which causes the stream to become closed.
        # (Note: close_message is np.NaN for StreamArray.)
        self.close_message = _close

    def reader(self, r, start_index=0):
        """
        Register a reader, r

        The newly registered reader starts reading list recent
        from index start, i.e., reads the slice
        recent[start_index:s.stop]
        If reader has already been registered with this stream
        its start value is updated to the parameter in the call.
        """
        self.start[r] = start_index

    def delete_reader(self, reader):
        """
        Delete this reader from this stream.
        """
        if reader in self.start:
            del self.start[reader]

    def call(self, agent):
        """
        Register a subscriber for this stream.
        """
        self.subscribers_set.add(agent)

    def delete_caller(self, agent):
        """
        Delete a subscriber for this stream.
        """
        self.subscribers_set.discard(agent)

    def append(self, value):
        """
        Append a single value to the end of the
        stream.
        """
        if self.closed:
            raise Exception("Cannot write to a closed stream.")
        self.recent[self.stop] = value
        self.stop += 1

        # Manage the list recent.
        # Set up a new version of the list
        # (if necessary) to prevent the list
        # from getting too long.
        if self.stop >= len(self.recent) - self._buffer_size:
            self._set_up_new_recent()
        
        # Inform subscribers that the stream has been
        # modified.
        for a in self.subscribers_set:
            a.next()

    def extend(self, value_list):
        """
        Extend the stream by the list of values, value_list.

        Parameters
        ----------
            value_list: list
        """
        if self.closed:
            raise Exception("Cannot write to a closed stream.")

        # Since this stream is a regular Stream (i.e.
        # implemented as a list) rather than Stream_Array
        # (which is implemented as a NumPy array), convert
        # any NumPy arrays to lists before inserting them
        # into the stream. See StreamArray for dealing with
        # streams implemented as NumPy arrays.
        if isinstance(value_list, np.ndarray):
            value_list = list(value_list)
        assert (isinstance(value_list, list)), 'value_list =' + str(value_list)

        if len(value_list) == 0:
            return
        
        value_list = remove_novalue_and_open_multivalue(value_list)
        if self.close_message in value_list:
            # Since close_message is in value_list, first output
            # the messages in value_list up to and including 
            # the message close_message and then close the stream.
            # close_flag indicates that this stream must
            # be closed after close_message is output
            close_flag = True
            # Value_list is up to, but not including, close_message
            value_list = value_list[:value_list.index(self.close_message)]
        else:
            close_flag = False

        self.new_stop = self.stop + len(value_list)
        if self.new_stop >= len(self.recent) - self._buffer_size:
            self._set_up_new_recent(self.new_stop)
        self.recent[self.stop: self.stop + len(value_list)] = value_list
        
        ## print 'In Stream.py. In extend(). stop = ', self.stop
        ## print 'stream = ', self.name
        ## print 'len(value_list) = ', len(value_list)
        ## print 'len(self.recent) = ', len(self.recent)
        ## print
        
        self.stop += len(value_list)
        # Inform subscribers that the stream has been
        # modified.
        for a in self.subscribers_set:
            a.next()

        # Close the stream if close_flag was set to True
        # because a close_message value was added to the stream.
        if close_flag:
            self.close()

    def set_name(self, name):
        self.name = name

    def print_recent(self):
        print self.name, '=', self.recent[:self.stop]

    def close(self):
        """
        Close this stream."
        """
        if self.closed:
            return
        print 'The stream, ' + self.name + ", in a process is closed."
        self.closed = True
        # signal subscribers that the stream has closed.
        # for a in self.subscribers_set: a.signal()

    def set_start(self, reader, starting_value):
        """ The reader tells the stream that it is only accessing
        elements of the list recent with index start or higher.

        """
        ## if 'displacement' in self.name:
        ##     print 'In Stream.py'
        ##     print 'in set_start'
        ##     print 'stream = ', self.name
        ##     print 'reader = ', reader.name
        ##     print 'starting_value = ', starting_value
        
        self.start[reader] = starting_value
        ## if 'displacement' in self.name:
        ##     for key, value in self.start.iteritems():
        ##         print 'agent name = ', key.name
        ##         print 'starting value = ', value


    def get_latest(self):
        if self.stop == 0:
            return []
        else:
            return self.recent[self.stop - 1]

    def get_last_n(self, n):
        return self.recent[max(self.stop-n, 0) : self.stop]
    
    def get_latest_n(self, n):
        return self.recent[max(self.stop-n, 0) : self.stop]

    def is_empty(self):
        return self.stop == 0

    def get_contents_after_column_value(self, column_number, value):
        assert(isinstance(column_number, int))
        assert(column_number >= 0)
        try:
            start_index = np.searchsorted(
                [row[column_number] for row in self.recent[:self.stop]], value)
            if start_index >= self.stop:
                return []
            else:
                return self.recent[start_index:self.stop]
        except:
            print 'In Stream.py. In get_contents_after_column_value()'
            print 'column_number =', column_number
            print 'value =', value
            raise
        return

    def get_index_for_column_value(self, column_number, value):
        assert(isinstance(column_number, int))
        try:
            start_index = np.searchsorted(
                [row[column_number] for row in self.recent[:self.stop]], value)
            if start_index >= self.stop:
                return -1
            else:
                return start_index
        except:
            print 'column_number =', column_number
            print 'value =', value
            raise
        return

    def _set_up_new_recent(self, new_stop=None):
        """
        Implement a form of garbage collection for streams
        by updating the list recent to delete elements of
        the list that are not accessed by any reader.
        """
        if not new_stop:
            new_stop = self.stop
        else:
            if new_stop < self.stop:
                print 'In Stream.py. In set_up_new_recent'
                print 'new_stop', new_stop, 'self.stop', self.stop
                print 'Error'
        # _begin = 0 if start is the empty dict,
        # else _begin = min over all r of start[r]
        self._begin = (0 if self.start == {}
                       else min(self.start.itervalues()))
        ## if self._begin == 0:
            ## print 'In Stream.py'
            ## print 'In set_up_new_recent'
            ## print 'stream = ', self.name
            ## print 'begin == 0'
            ## print 'new_stop = ', new_stop
            ## print 'len(self.recent) = ', len(self.recent)
            ## print 'buffer size = ', self._buffer_size
            ## ## print 'self.start = ', self.start
            ## for key, value in self.start.iteritems():
            ##     print 'agent name = ', key.name
            ##     print 'starting value = ', value
            ## print
        
        # If new_stop < len(recent) - _buffer_size then recent has
        # enough empty slots to add the values appended to
        # the stream; so no need to change recent.
        if new_stop < len(self.recent) - self._buffer_size:
            return

        # recent does not have enough empty slots for the
        # new values appended to the stream. So, create a
        # new recent.
        # If some reader is slow compared to the rate at which
        # values are appended to the stream, then _begin is
        # small compared to _buffer_size. On the other hand, if
        # all readers are moving the windows that they read
        # forward as fast as values are appended to the stream
        # then _begin will be large, almost equal to len(recent).
        # If some reader is very slow, then double the size of
        # recent but don't double _buffer_size
        
        ## print 'In Stream.py. New recent'
        ## print 'current begin = ', self._begin
        ## print 'new_stop = ', new_stop
        ## print 'buffer_size = ', self._buffer_size
        ## print 'current buffer_size = ', self._buffer_size
        
        # The size of the active part of the stream
        # is new_stop - self._begin.
        # If the active part fits into the buffer size
        # then leave the size of recent and the buffer size
        # unchanged.
        if new_stop - self._begin <= self._buffer_size:
            # All readers are keeping up with the stream
            # Do not change _buffer_size or length of recent
            new_size = len(self.recent)
        else:
            # Some readers (consumers) are not keeping up with
            # the writers (the producers) of the stream. The
            # number of active entries in the stream exceeds the buffer
            # size. So double the size of recent and double the
            # buffer size.
            new_size = len(self.recent) * 2
            self._buffer_size *= 2
            print 'DOUBLING'
            print 'new_stop = ', new_stop
            print 'new size = ', new_size
            print 'new buffer size = ', self._buffer_size
            print 'stream = ', self.name
            print 'self._begin =', self._begin
            print 'number items in stream = ', self.stop - self._begin
            for key, value in self.start.iteritems():
                print 'agent = ', key
                print 'agent name = ', key.name
                print 'starting value = ', value
        
        # 0 is the padding value.
        self.new_recent = self._create_recent(new_size)

        # Copy the values that readers can read, and ONLY those
        # values into new_recent. Readers do not read values in
        # recent with indexes smaller than _begin, and recent has
        # no values with indexes greater than stop.
        self.new_recent[:new_stop - self._begin] = \
            self.recent[self._begin: new_stop]
        # Copy new_recent into recent
        #self.recent = list(self.new_recent)
        self.recent, self.new_recent = self.new_recent, self.recent
        del self.new_recent
        # Maintain the invariant recent[i] = stream[i + offset]
        # by incrementing offset since messages in new_recent were
        # shifted left (earlier) from the old recent by offset
        # number of slots.
        self.offset += self._begin

        # A reader reading the value in slot l in the old recent
        # will now read the same value in slot (l - _begin) in
        # new_recent.
        for key in self.start.iterkeys():
            self.start[key] -= self._begin
        self.stop -= self._begin
        self._begin = 0

        

    def _create_recent(self, size):
        return [0] * size

    
##########################################################
##########################################################
class StreamArray(Stream):
    def __init__(self, name="NoName", proc_name="UnknownProcess",
                 dimension=0,
                 dtype=float,
                 stream_size=DEFAULT_STREAM_SIZE,
                 buffer_size = DEFAULT_BUFFER_SIZE_FOR_STREAM):
        
        assert(isinstance(stream_size, int) and stream_size > 0)
        assert(isinstance(buffer_size, int) and buffer_size > 0)
        assert((isinstance(dimension, int) and dimension >=0) or
               ((isinstance(dimension, tuple) or
                isinstance(dimension, list) or
                isinstance(dimension, np.ndarray) and
                all(isinstance(v, int) and v > 0 for v in dimension)))
               )
        
        self._buffer_size = buffer_size
        self.name = name
        # Name of the process in which this stream lives.
        self.proc_name = proc_name
        # dimension is either 0, a positive integer, or a nonempty
        # tuple or list or array of positive integers.
        # If dimension is 0, then:
        # each row of the stream is an an element of type dtype.
        # If dimension is a positive integer, then:
        # each row of the stream is a 1-D array consisting of dimension
        # elements. Each element is of data type, dtype.
        # If dimension is a tuple then it must be a tuple of positive
        # integers. In this case, each row of the stream is an
        # N-dimensional array where N is the length of the tuple.
        # dimension is the dimensions of this array.
        self.dimension = dimension
        self.dtype = dtype
        # Create the array recent and the parameters
        # associated with garbage collecting elements in the stream.
        # Pad recent with any padded value (e.g. zeroes).
        # The array recent consists of the most recent rows of the
        # stream.
        self.recent = self._create_recent(stream_size)
        self._begin = 0
        # Initially offset is 0 because the array recent is being 
        # read from the beginning
        self.offset = 0
        # Initially, the stream is empty. So, the array recent has
        # 0 values.
        self.stop = 0
        # Initially the stream has no readers.
        self.start = dict()
        # Initially the stream has no subscribers.
        self.subscribers_set = set()
        # Initially the stream is open
        self.closed = False
        # The command to close the stream is np.NaN
        # which is 'not a number'
        self.close_message = np.NaN

    def _create_recent(self, size):
        if self.dimension is 0:
            return np.zeros(size, self.dtype)
        elif isinstance(self.dimension, int):
            return np.zeros([size, self.dimension], self.dtype)
        else:
            d = list(self.dimension)
            d.insert(0, self.size)
            return np.zeros(d, self.dtype)
        

    def extend(self, lst):
        """
        See extend() for the class Stream.
        Extend the stream by an numpy ndarray.

        Parameters
        ----------
            lst: np.ndarray or list
               a may be a multidimensional array.
        """
        if _no_value in lst:
            return
        if self.closed:
            raise Exception("Cannot write to a closed stream.")
        if isinstance(lst, _array): lst = [lst]
        if any(isinstance(element, _array) for element in lst):
            for element in lst:
                a = element.value
                # Check the type of a.
                assert(isinstance(a, np.ndarray)),\
                        "Expect extension of a numpy stream to be a numpy ndarray,\
                        not '{0}' ".format(a)
                if self.dimension == 0:
                    if len(a.shape) != 1:
                        raise TypeError('Expect array shape {0} == 1'.format(a.shape))
                if isinstance(self.dimension, int) and self.dimension > 0:
                    if a.shape[1] != self.dimension:
                        raise TypeError(
                            'Expect number of columns in array {0} == StreamArray dimension {1}:'.format(
                                a.shape[1], self.dimension))
                if (isinstance(self.dimension, tuple) or
                    isinstance(self.dimension, list) or
                    isinstance(self.dimension, np.ndarray)):
                    if a.shape[1:] != self.dimension:
                        raise TypeError(
                            'Expect a.shape[1:] {0} == dimension {1}:'.format(a.shape, self.dimension))

            ## if self.close_message in a:
            ##     close_flag = True
            ##     a = a[:a.index(self.close_message)]
            ## else:
            ##     close_flag = False
                if len(a) > 0:
                    new_stop = self.stop + len(a)
                    if new_stop >= len(self.recent) - self._buffer_size:
                        self._set_up_new_recent(new_stop)
                    ## if self.stop + len(a) > len(self.recent):
                    ##     self._set_up_new_recent()
                    try:
                        self.recent[self.stop: self.stop + len(a)] = a
                    except:
                        print 'In Stream.py, StreamArray. extend()'
                        print 'stream = ', self.name
                        print 'self.stop = ', self.stop
                        print 'len(a) = ', len(a)
                        print 'len(self.recent) = ', len(self.recent)
                    self.stop = self.stop + len(a)
                
            # Finished loop: for element in lst
        else:
            if len(lst) > 0:
                new_stop = self.stop + len(lst)
                if new_stop > len(self.recent) - self._buffer_size:
                        self._set_up_new_recent(new_stop)
                # TODO: CONVERT TO TRY, EXCEPT to catch type of lst
                try:
                    for i in range(len(lst)):
                        self.recent[self.stop+i] = lst[i]
                except Exception:
                    value_appended = lst[i]
                    print 'stream array name: ', self.name
                    print 'type of values added to the stream array is ', type(value_appended)
                    if len(value_appended) == 0:
                        print 'appending zero-length value'
                    else:
                        print 'appending', value_appended
                        print 'self.recent[self.stop+i -1]', self.recent[self.stop+i -1]
                    logging.error('Error adding values to stream %s')
                    if type(value_appended) is not np.ndarray:
                        raise TypeError()
                    
                self.stop = self.stop + len(lst)
                if self.stop >= len(self.recent) - self._buffer_size:
                        self._set_up_new_recent()
                
        for subscriber in self.subscribers_set:
            subscriber.next()
        #self._set_up_new_recent()

    def get_contents_after_time(self, start_time):
        try:
            start_index = np.searchsorted(self.recent[:self.stop]['time'], start_time)
            if start_index >= self.stop:
                return np.zeros(0, dtype=self.dtype)
            else:
                return self.recent[start_index:self.stop]
        except:
            print 'start_time =', start_time
            print 'self.dtype =', self.dtype
            raise

        return

class StreamTimed(StreamArray):
    def __init__(self):
        StreamArray.__init__(name)

class StreamSeries(Stream):
    def __init__(self, name=None):
        super(StreamSeries, self).__init__(name)

    def _create_recent(self, size): return pd.Series([np.nan] * size)
