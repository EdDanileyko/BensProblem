# -*- coding: utf-8 -*-
"""
Created on Sat Jan 19 17:44:31 2019

@author: edwar
"""
import re
import types

from collections import (
        namedtuple,
        deque,
        defaultdict
)
from datetime    import datetime
from functools   import partial
from random      import choice
#from time        import sleep


def generateLogLines(n: int) -> 'log line generator':
    """Returns generator yielding log lines with format:
    timestamp : user : endpoint : method : statuscode
    
    args:
        int n - number of log lines to generate
        
    >>> log = logLineGenerator(1000)
    >>> type(log)
    generator
    >>>
    """
    if not isinstance(n, int):
        raise TypeError('requires int(n) to initialize')
        exit(1)
        
    print('Generating log lines...')
        
    newTimestamp = partial(datetime.strftime, format='%H:%M:%S.%f3')
    
    users       = ('Graham Chapman', 'John Cleese',
                   'Eric Idle', 'Terry Gilliam',
                   'Michael Palin', 'Terry Jones',
                   'Ian Davidson')
    endpoints   = ('/snakes', '/babysnakes', '/fullgrownsnakes',
                   '/venomous', '/constrictors', '/watersnakes',
                   '/miscsnakes')
    methods     = ('GET', 'POST', 'PUT', 'DELETE')
    statusCodes = (200, 400, 404, 500)
    
    for _ in range(n):
        #sleep(0.00001)
        yield ' : '.join([newTimestamp(datetime.now()),
                          choice(users),
                          choice(endpoints),
                          choice(methods),
                          str(choice(statusCodes))])
    yield 20
    yield 'bogus string'
    
    
def getEndpointData(log:    list, *,
                    seqlen: int,
                    delim:  str) -> "userEndpoints, seqCounts, maxSeq":
    """Passes over a log and tracks the last indicated number of endpoint
    visits for any user, checking against the master count of those same n
    endpoints.
    
    args:
        log - log lines to process in either sequence or generator type
        endpointseq - int(length of endpoint sequence to track)
        delim - str(log file delimiter)    
    
    returns:
        1. The {user: [endpointLogs]} dict.
        2. The {endpoint: visitCount} dict
        3. The tuple(endpoints) visited the most frequently in sequence.
        
    >>> userEndpoints, seqCounts, maxSeq = getEndpointData(log, delim=' : ')
    """
    if not isinstance(log, (list, tuple, types.GeneratorType)):
        raise TypeError('Requires that input be sequence type to initialize')
    if not isinstance(delim, str):
        raise TypeError('Delimiter is not a string')
    if not isinstance(seqlen, int):
        raise TypeError('Must use int for length of endpoint visit sequence')
    
    print('Parsing log...')
    
    Record = namedtuple('Record', 'timestamp user endpoint method statuscode')
    
    # initialize dictionaries of {user: sequence of maxlen=seqlen}
    # and {sequence: count of visits initialized at 0}
    userEndpoints = defaultdict(lambda: deque(maxlen=seqlen))
    seqCounts = defaultdict(int)
    maxSeq = None
    
    for num, line in enumerate(log, start=1):
        print('[INFO] Parsing: {0}'.format(line))
        
        # Attempt to create an endpoint record
        try:
            rec = Record(*re.split(delim, line))
        except:
            print('[WARN] Cannot process line {0}: Found {1}({2}).' \
                  .format(str(num), type(line).__name__, str(line)))

        # Prepend the associated user's deque of endpoint visits
        userEndpoints[rec.user].appendleft(rec.endpoint)
        
        # If the user's deque is filled, increment the count of this sequence
        # of endpoint visits
        if len(userEndpoints[rec.user]) == seqlen:
            currSeq = tuple(userEndpoints[rec.user])
            seqCounts[currSeq] += 1
            
            # Check the current sequence's count of visits against the sequence
            # with the highest number of visits and set as highest if greater
            if maxSeq is None:
                maxSeq = currSeq
            elif seqCounts[maxSeq] < seqCounts[currSeq]:
                maxSeq = currSeq
                
    if maxSeq is None:
        print('Failure to calculate data')
        return None, None, None
        
    print('\nCalculated {0} visits to the following endpoint sequence:' \
          .format(seqCounts[maxSeq]))
    print(' -> '.join(maxSeq))
    
    return userEndpoints, seqCounts, maxSeq


def main():
    loglines = generateLogLines(10_000)
    return getEndpointData(loglines, seqlen=5, delim=' : ')
    

if __name__ == '__main__':
    userEndpoints, seqCounts, maxSeq = main()                
