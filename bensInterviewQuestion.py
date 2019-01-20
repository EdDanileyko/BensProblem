# -*- coding: utf-8 -*-
"""
Created on Sat Jan 19 17:44:31 2019

@author: edwar
"""
from collections import namedtuple
from datetime    import datetime
from functools   import partial
from random      import choice
from time        import sleep


def logLineGenerator(n: int):
    """Returns generator yielding logs with "timestamp : user : endpoint" per
    line to microsecond precision
    
    args:
        int n
        
    >>> log = logLineGenerator(1000)
    >>> type(log)
    generator
    >>>
    """
    newTimestamp = partial(datetime.strftime, format='%H:%M:%S.%f')
    
    users = ('Graham Chapman',
             'John Cleese',
             'Eric Idle',
             'Terry Gilliam',
             'Michael Palin',
             'Terry Jones',
             'Ian Davidson')
    
    endpoints = ('/',
                 '/login',
                 '/auth',
                 '/home',
                 '/account',
                 '/logout',
                 '/register')
    
    for _ in range(n):
        sleep(0.0001)
        yield ' : '.join([newTimestamp(datetime.now()),
                          choice(users),
                          choice(endpoints)])

    
def getEndpointTriplesDictAndMaxTriple(log: list) -> tuple:
    """Passes over a log and tracks user endpoint visits, checking the last
    three enpoints which are visited against the master count of that endpoint.
    
    returns the three endpoints visited the most frequently in sequence
    """
    Record = namedtuple('Record', ('timestamp', 'user', 'endpoint'))
    userEndpoints = {}
    tripleCounts  = {}
    
    maxTriple = None
    
    for line in log:
        print(line)
        rec = Record(*line.split(' : '))
        
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
    
    endpoints, triples, maxTriple = getEndpointTriplesDictAndMaxTriple(log)
    
    print('\nCalculated {0} visits to the following endpoint sequence:' \
          .format(str(triples[maxTriple])))
    print(' -> '.join(maxTriple))
    
    return endpoints, triples, maxTriple


if __name__ == '__main__':
    endpoints, triples, maxTriple = main()                