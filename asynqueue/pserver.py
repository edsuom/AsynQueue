# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker/manager interface.
#
# Copyright (C) 2006-2007 by Edwin A. Suominen, http://www.eepatents.com
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the file COPYING for more details.
# 
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA

"""
This module is imported by a subordinate Python process to service
a ProcessWorker.
"""

import sys, traceback

from twisted.internet import reactor
from twisted.internet.protocol import Factory
from twisted.protocols import amp

import util
from util import o2p, p2o


class RunTask(amp.Command):
    """
    Runs a task and returns the status and result. The callable, args,
    and kw are all pickled strings. The args and kw can be empty
    strings, indicating no args or kw.

    The response has the following status/result structure:

    'e': An exception was raised; the result is a pretty-printed
         traceback string.

    'r': Ran fine, the result is the pickled return value of the call.

    'i': Ran fine, but the result is an iterable other than a standard
         Python one. The result is an ID string to use for your
         calls to C{GetMore}.

    'c': Ran fine, but the result is too big for a single return
         value. So you get an ID string for calls to C{GetMore}.

    """
    arguments = [
        ('f', amp.String()),
        ('args', amp.String()),
        ('kw', amp.String()),
    ]
    response = [
        ('status', amp.String()),
        ('result', amp.String()),
    ]


class GetMore(amp.Command):
    """
    With a unique ID, get the next iteration of data from an iterator
    or a task result so big that it had to be chunked.

    The response has a 'value' string with the pickled iteration value
    or a chunk of the too-big task result, an 'isValid' bool (which
    should be True for all cases except an iterable that's empty from
    the start), and a 'moreLeft' Bool indicating if you should call me
    again with this ID for another iteration or chunk.
    """
    arguments = [
        ('ID', amp.String())
    ]
    response = [
        ('value', amp.String()),
        ('isValid', amp.Boolean()),
        ('moreLeft', amp.Boolean()),
    ]


class QuitRunning(amp.Command):
    """
    Shutdown the reactor (after I'm done responding) and exit.
    """
    arguments = []
    response = []


class ChunkyResult(object):
    def __init__(self, bigString, chunkSize):
        self.bigString = bigString
        self.chunkSize = chunkSize
        self.done = False
    
    def __iter__(self):
        return self

    def next(self):
        if self.done:
            raise StopIteration
        thisChunk = self.bigString[:self.chunkSize]
        if self.chunkSize < len(self.bigString):
            self.bigString = self.bigString[self.chunkSize:]
        else:
            self.done = True
        return thisChunk
        

class TaskServer(amp.AMP):
    """
    The AMP protocol for running tasks, entirely unsafely.
    """
    pfTable = {}
    shutdownDelay = 1.0
    maxChunkSize = 2**16 - 1

    @classmethod
    def started(cls):
        """
        Now that the reactor is running, sends the "OK" handshaking code
        to the ProcessWorker in my spawning Python interpreter via
        stdout.

        Might do something with hashing an authentication code here
        someday. Or not.
        """
        print "OK"
    
    def _getID(self, *args):
        ID = hash(args)
        if ID not in self.pfTable:
            self.pfTable[ID] = util.Prefetcherator()
        return ID
    
    def _tryTask(self, f, *args, **kw):
        response = {}
        try:
            result = f(*args, **kw)
        except Exception as e:
            response['status'] = 'e'
            response['result'] = util.callTraceback(e, f, *args, **kw)
        else:
            if util.Deferator.isIterator(result):
                # An iterator
                # ------------------------------
                # Prefetcherators don't use much memory, keep one
                # around for each function-args combo
                ID = self._getID(f, args)
                # Try the iterator on for size
                if self.pfTable[ID].setIterator(result):
                    response['status'] = 'i'
                    response['result'] = ID
                else:
                    # Aw, can't do the iteration, try an
                    # iterator-as-list fallback
                    try:
                        pickledResult = list(result)
                    except:
                        response['status'] = 'e'
                        response['result'] = \
                            "Failed to iterate for task {}".format(
                                util.callInfo(f, *args, **kw))
            else:
                # Just a regular result...
                pickledResult = o2p(result)
            if 'pickledResult' in locals():
                if len(pickledResult) > self.maxChunkSize:
                    # ...too big to send as a single pickled result
                    response['status'] = 'c'
                    ID = self._getID(f, args)
                    self.pfTable[ID].setIterator(
                        ChunkyResult(pickledResult, self.maxChunkSize))
                    response['result'] = ID
                else:
                    # ...small enough to just send
                    response['status'] = 'r'
                    response['result'] = pickledResult
        return response
    
    def runTask(self, f, args, kw):
        f = p2o(f)
        args = p2o(args, [])
        kw = p2o(kw, {})
        return self._tryTask(f, *args, **kw)
    RunTask.responder(runTask)

    def getMore(self, ID):
        value, isValid, moreLeft = self.pfTable[ID].getNext()
        return {'value': value, 'isValid': isValid, 'moreLeft': moreLeft}
    GetMore.responder(getMore)
    
    def quitRunning(self):
        reactor.callLater(self.shutdownDelay, reactor.stop)
        return {}
    QuitRunning.responder(quitRunning)


def start(address):
    pf = Factory()
    pf.protocol = TaskServer
    # Currently the security of UNIX domain sockets is the only
    # security we have!
    reactor.listenUNIX(address, pf)
    reactor.callWhenRunning(TaskServer.started)
    reactor.run()


if __name__ == '__main__':
    address = sys.argv[-1]
    start(address)
    
