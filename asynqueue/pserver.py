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


class ChunkyString(object):
    """
    I iterate chunks of a big string, deleting my internal reference
    to it when done so it can be garbage collected even if I'm not.
    """
    chunkSize = 2**16 - 1

    def __init__(self, bigString):
        self.k0 = 0
        self.N = len(bigString)
        self.bigString = bigString
    
    def __iter__(self):
        return self

    def next(self):
        if not hasattr(self, 'bigString'):
            raise StopIteration
        k1 = min([self.k0 + self.chunkSize, self.N])
        thisChunk = self.bigString[self.k0:k1]
        if k1 == self.N:
            del self.bigString
        else:
            self.k0 = k1
        return thisChunk


class TaskUniverse(object):
    """
    I am the universe for all tasks running with a particular
    connection to my L{TaskServer}.
    """
    def __init__(self):
        self.pfs = {}
        self.info = util.Info()
        self.dt = util.DeferredTracker()

    def _oops(self, failureObj):
        self.response['status'] = 'e'
        self.response['result'] = self.info(failureObj)

    def _pf(self, ID=None):
        """
        Returns the Prefetcherator of record for a particular ID if one is
        specified. Otherwise returns a unique Prefetcherator instance
        for the current f-args-kw combo, constructing a new one if
        necessary.
        """
        if ID is None:
            ID = self.info.getID()
            if ID not in self.pfs:
                self.pfs[ID] = util.Prefetcherator(ID)
        return self.pfs[ID]
    
    def _iteratorResult(self, result):
        """
        Handles a result that is an iterator.

        Prefetcherators use hardly any memory, so we keep one around
        for each f-args-kw combo resulting in calls to this method.
        """
        # Try the iterator on for size
        if self._pf().setIterator(result):
            self.response['status'] = 'i'
            self.response['result'] = ID
            return
        # Aw, can't do the iteration, try an iterator-as-list fallback
        try:
            pickledResult = list(result)
        except:
            self.response['status'] = 'e'
            text = "Failed to iterate for task {}".format(
                self.info.aboutCall())
            self.response['result'] = text
        else:
            self.pickledResult(pickledResult)

    def _pickledResult(self, pickledResult):
        """
        Handles a regular result that's been pickled for transmission.
        """
        if len(pickledResult) > ChunkyResult.chunkSize:
            # Too big to send as a single pickled result
            self.response['status'] = 'c'
            pf = self._pf()
            pf.setIterator(ChunkyResult(pickledResult))
            self.response['result'] = pf.ID
        else:
            # Small enough to just send
            self.response['status'] = 'r'
            self.response['result'] = pickledResult

    def call(self, f, *args, **kw):
        def ran(result):
            if isinstance(result, util.Deferator):
                # Result is a Deferator, just tell my prefetcherator
                # to use its getNext function.
                df, dargs, dkw = result.callTuple
                self._pf().setNextCallable(df, *dargs, **dkw)
            elif util.Deferator.isIterator(result):
                # It's a Deferator-able iterator
                self._iteratorResult(result)
            else:
                # It's a regular result
                self._pickledResult(o2p(result))
            return self.response
                    
        self.response = {}
        self.info.setCall(f, args, kw)
        if kw.pop('thread', False):
            if not hasattr(self, 't'):
                self.t = util.ThreadLooper()
            d = self.t.deferToThread(f, *args, **kw)
        else:
            d = defer.maybeDeferred(f, *args, **kw)
        self.dt.put(d)
        d.addCallbacks(ran, self._oops)
        return d

    def shutdown(self):
        if hasattr(self, 't'):
            d = self.t.stop()
            self.dt.put(d)
        return self.dt.deferToAll()


class TaskServer(amp.AMP):
    """
    The AMP server protocol for running tasks, entirely unsafely.
    """
    multiverse = {}
    shutdownDelay = 1.0

    def makeConnection(self, transport):
        self.u = TaskUniverse()
        self.multiverse[id(transport)] = self.u
        return super(TaskServer, self).makeConnection(transport)
    
    def connectionLost(self, reason):
        super(TaskServer, self).connectionLost(reason)
        return self.quitRunning()

    def runTask(self, f, args, kw):
        """
        Responder for L{RunTask}
        """
        f = p2o(f)
        args = p2o(args, [])
        kw = p2o(kw, {})
        return self.u.call(f, *args, **kw)
    #------------------------------------------------------------------
    RunTask.responder(runTask)

    def getMore(self, ID):
        """
        Responder for L{GetMore}
        """
        def done(result):
            for k, value in enumerate(result):
                name = GetMore.response[k][0]
                response[name] = value

        def oops(failureObj):
            response['value'] = None
            response['isValid'] = False
            response['moreLeft'] = False

        response = {}
        return defer.maybeDeferred(
            self.pfs[ID].getNext).addCallbacks(done, oops)
    #------------------------------------------------------------------
    GetMore.responder(getMore)
    
    def quitRunning(self):
        """
        Responder for L{QuitRunning}
        """
        def stopped(null):
            reactor.callLater(self.shutdownDelay, reactor.stop)
            return {}
        return defer.DeferredList([
            u.shutdown()
            for u in self.multiverse.itervalues()]).addBoth(stopped)
    #------------------------------------------------------------------
    QuitRunning.responder(quitRunning)



class TaskFactory(Factory):
    protocol = TaskServer
                 
    def startFactory(self):
        """
        Now that I'm ready to accept connections, sends the "OK"
        handshaking code to the ProcessWorker in my spawning Python
        interpreter via stdout.

        Might do something with hashing an authentication code here
        someday. Or not.
        """
        print "OK"
                 

def start(address):
    pf = TaskFactory()
    # Currently the security of UNIX domain sockets is the only
    # security we have!
    reactor.listenUNIX(address, pf)
    reactor.run()


if __name__ == '__main__':
    address = sys.argv[-1]
    start(address)
    
