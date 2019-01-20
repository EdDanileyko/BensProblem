# -*- coding: utf-8 -*-
"""
Created on Sat Jan 19 17:44:31 2019

@author: edwar
"""
import re
import types

from collections import namedtuple
from datetime    import datetime
from functools   import partial
from random      import choice
from time        import sleep


def logLineGenerator(n: int):
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
    endpoints   = ('/', '/login', '/auth', '/home',
                   '/account', '/logout', '/register')
    methods     = ('GET', 'POST', 'PUT', 'DELETE')
    statusCodes = (200, 400, 404, 500)
    
    for _ in range(n):
        sleep(0.0001)
        yield ' : '.join([newTimestamp(datetime.now()),
                          choice(users),
                          choice(endpoints),
                          choice(methods),
                          str(choice(statusCodes))])
    yield 20

    
def getEndpointData(log: list, *, delim: str) -> tuple:
    """Passes over a log and tracks user endpoint visits, checking the sequence
    of three endpoints which are last visited against the master count of those
    same three endpoints.
    
    args:
        log - log lines to process
    
    returns:
        1. The {user: [endpointLogs]} dict.
        2. The {endpoint: visitCount} dict
        3. The tuple(three endpoints) visited the most frequently in sequence.
        
    >>> userEndpoints, tripleCounts, maxTriple = getEndpointData(log, delim=' : ')
    """
    if not isinstance(log, (list, tuple, types.GeneratorType)):
        raise TypeError('Requires that input be sequence type to initialize')
        exit(1)
    if not isinstance(delim, str):
        raise TypeError('Delimiter is not a string')
        exit(1)
    
    Record = namedtuple('Record',('timestamp user endpoint method statuscode'))
    userEndpoints = {}
    tripleCounts  = {}
    
    maxTriple = None
    
    isString = lambda s: isinstance(s, str)
    canParse = lambda s: re.search(delim, s) and len(re.split(delim, s)) == 5
    
    for num, line in enumerate(log):
        if not isString(line) or not canParse(line):
            print('Cannot parse data at line {0}:\nFound {1}({2})\nIgnoring.' \
                  .format(str(num), type(line).__name__, str(line)))
            continue
            
        print('Parsing - {0}'.format(line))
        rec = Record(*line.split(delim))
        
        if userEndpoints.get(rec.user) is None:
            userEndpoints[rec.user] = [rec.endpoint]
        else:
            userEndpoints[rec.user].append(rec.endpoint)
            
        if len(userEndpoints[rec.user]) >= 3:
            currTriple = tuple(userEndpoints[rec.user][-3:])
            
            if tripleCounts.get(currTriple) is None:
                tripleCounts[currTriple] = 1
            else:
                tripleCounts[currTriple] += 1
                
                if maxTriple is None:
                    maxTriple = currTriple
                elif tripleCounts[maxTriple] < tripleCounts[currTriple]:
                    maxTriple = currTriple
                
    return userEndpoints, tripleCounts, maxTriple


def main():
    print('Generating log lines...')
    
    log = logLineGenerator(1000)
    
    print('Parsing log...')
    
    userEndpoints, triples, maxTriple = getEndpointData(log, delim=' : ')
    
    if maxTriple is None:
        print('Failure to parse logfile')
        return None, None, None
        
    print('\nCalculated {0} visits to the following endpoint sequence:' \
          .format(str(triples[maxTriple])))
    print(' -> '.join(maxTriple))
    
    return userEndpoints, triples, maxTriple


if __name__ == '__main__':
    userEndpoints, triples, maxTriple = main()                
