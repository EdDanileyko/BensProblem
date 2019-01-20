# -*- coding: utf-8 -*-
"""
Created on Sat Jan 19 17:44:31 2019

@author: edwar
"""
import re
import types

from collections import (
        namedtuple,
        deque
)
from datetime    import datetime
from functools   import partial
from random      import choice
from time        import sleep


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
        
    newTimestamp = partial(datetime.strftime, format='%H:%M:%S.%f')
    
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
        #sleep(0.0001)
        yield ' : '.join([newTimestamp(datetime.now()),
                          choice(users),
                          choice(endpoints),
                          choice(methods),
                          str(choice(statusCodes))])
    yield 20
    yield 'bogus string'

    
    
def getEndpointData(log: list, *, seqlen: int, delim: str) -> tuple:
    """Passes over a log and tracks the last indicated number of endpoint
    visits for any user, checking against the master count of those same three
    endpoints.
    
    args:
        log - log lines to process in either sequence or generator type
        endpointseq - int(length of endpoint sequence to track)
        delim - str(log file delimiter)    
    
    returns:
        1. The {user: [endpointLogs]} dict.
        2. The {endpoint: visitCount} dict
        3. The tuple(three endpoints) visited the most frequently in sequence.
        
    >>> userEndpoints, seqCounts, maxSeq = getEndpointData(log,
                                                        delim=' : ',
                                                        seqlen=3)
    """
    if not isinstance(log, (list, tuple, types.GeneratorType)):
        raise TypeError('Requires that input be sequence type to initialize')
    if not isinstance(delim, str):
        raise TypeError('Delimiter is not a string')
    if not isinstance(seqlen, int):
        raise TypeError('Must use int for length of endpoint visit sequence')
    
    Record = namedtuple('Record', 'timestamp user endpoint method statuscode')
    userEndpoints = {}
    seqCounts  = {}
    
    maxSeq = None
    
    isString = lambda s: isinstance(s, str)
    canParse = lambda s: re.search(delim, s) and len(re.split(delim, s)) == 5
    
    for num, line in enumerate(log, start=1):
        if not isString(line) or not canParse(line):
            print('Cannot parse data at line {0}:\nFound {1}({2})\nIgnoring.' \
                  .format(str(num), type(line).__name__, str(line)))
            continue
            
        print('Parsing - {0}'.format(line))
        rec = Record(*line.split(delim))
        
        if userEndpoints.get(rec.user) is None:
            userEndpoints[rec.user] = deque([rec.endpoint], maxlen=seqlen)
        else:
            userEndpoints[rec.user].append(rec.endpoint)
            
        if len(userEndpoints[rec.user]) >= seqlen:
            currSeq = tuple(userEndpoints[rec.user])
            
            if seqCounts.get(currSeq) is None:
                seqCounts[currSeq] = 1
            else:
                seqCounts[currSeq] += 1
                
                if maxSeq is None:
                    maxSeq = currSeq
                elif seqCounts[maxSeq] < seqCounts[currSeq]:
                    maxSeq = currSeq
                
    return userEndpoints, seqCounts, maxSeq


def main():
    print('Generating log lines...')
    
    log = generateLogLines(10000)
    
    print('Parsing log...')
    
    userEndpoints, seqs, maxSeq = getEndpointData(log, seqlen=4, delim=' : ')
    
    if maxSeq is None:
        print('Failure to calculate data')
        return None, None, None
        
    print('\nCalculated {0} visits to the following endpoint sequence:' \
          .format(seqs[maxSeq]))
    print(' -> '.join(maxSeq))
    
    return userEndpoints, seqs, maxSeq


if __name__ == '__main__':
    userEndpoints, seqs, maxSeq = main()                
