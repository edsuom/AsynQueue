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
Implementors of the worker interface.
"""

from zope.interface import implements
from twisted.python import failure
from twisted.internet import defer, reactor, endpoints
from twisted.protocols import amp

import errors, util
from interfaces import IWorker
from pserver import RunTask, GetMore, QuitRunning


class ThreadWorker(object):
    """
    I implement an L{IWorker} that runs tasks in a dedicated worker
    thread.

    You can supply a series keyword containing a list of one or more
    task series that I am qualified to handle.
    """
    implements(IWorker)
    cQualified = []

    def __init__(self, series=[]):
        self.iQualified = series
        self.t = util.ThreadLooper()

    def setResignator(self, callableObject):
        """
        There's nothing that would make a thread worker resign on its own.
        """

    def run(self, task):
        f, args, kw = task.callTuple
        return self.t.deferToThread(
            f, *args, **kw).addCallback(task.d.callback)
    
    def stop(self):
        """
        The returned deferred fires when the task loop has ended and its thread
        terminated.
        """
        return self.t.stop()

    def crash(self):
        """
        Unfortunately, a thread can only terminate itself, so calling
        this method only forces firing of the deferred returned from a
        previous call to L{stop} and returns the task that hung the
        thread.
        """
        if self.task is not None and not self.task.d.called:
            result = [self.task]
        else:
            # This shouldn't happen
            result = []
        if hasattr(self.t, 'd') and not self.t.d.called:
            del self.task
            self.t.d.callback(None)
        self.t.stop()


class AsyncWorker(object):
    """
    I implement an L{IWorker} that runs tasks in the Twisted main
    loop, one task at a time but in a well-behaved non-blocking
    manner. If the task callable doesn't return a deferred, it better
    get its work done fast.

    You can supply a series keyword containing a list of one or more
    task series that I am qualified to handle.
    """
    def __init__(self, series=[]):
        self.iQualified = series

    def setResignator(self, callableObject):
        """
        I run in the main loop, so there's nothing that would make me
        resign on my own.
        """

    def _done(self, result):
        return (True, result)

    def _oops(self, failure):
        return (False, self.taskTraceback(failure.value))

    def run(self, task):
        self.checkReady()
        # Async workers are strictly one task at a time. What's the
        # point of this whole package otherwise?
        self.task = task
        f, args, kw = self.task.callTuple
        # This must not block!
        # ---------------------------------------------------------------------
        self.d = defer.maybeDeferred(f, *args, **kw)
        # ---------------------------------------------------------------------
        self.d.addCallbacks(self._done, self._oops)
        self.d.addCallback(task.callback)
        return self.d

    def stop(self):
        return self.deferToStop()

    def crash(self):
        """
        There's no point to implementing this because the Twisted main
        loop will block along with any task you give this worker.
        """
        

class ProcessWorker(object):
    """
    I implement an L{IWorker} that runs tasks in a subordinate Python
    interpreter via Twisted's Asynchronous Messaging Protocol.

    You can also supply a series keyword containing a list of one or
    more task series that I am qualified to handle.
    """
    pList = []

    class ProcessProtocol(object):
        def __init__(self, d, disconnectCallback):
            self.d = d
            self.disconnectCallback = disconnectCallback
        def makeConnection(self, process):
            pass
        def childDataReceived(self, childFD, data):
            if childFD == 1 and data == 'OK':
                self.d.callback(None)
        def childConnectionLost(self, childFD):
            self.disconnectCallback()
        def processExited(self, reason):
            self.disconnectCallback()
        def processEnded(self, reason):
            self.disconnectCallback()
    
    def __init__(self, N=1, reconnect=False, series=[]):
        # TODO: Allow N > 1
        if N > 1:
            raise NotImplementedError("Only one task at a time for now")
        self.reconnect = reconnect
        self.iQualified = series
        # The process number
        self.pNumber = len(self.pList)
        # The process name, which is used for the UNIX socket address
        self.pName = "worker-{:d}".format(self.pNumber)
        # The UNIX socket address
        self.address = os.path.join(
            os.tempnam(), "{}.sock".format(self.pName))
        # Start the server and make the connection
        self.dConnected = self._spawnAndConnect()

    @defer.inlineCallbacks
    def _spawnAndConnect(self):
        # Spawn the pserver and "wait" for it to indicate it's OK
        args = [sys.executable, "-m", "asynqueue.pserver", self.address]
        d = defer.Deferred()
        pP = self.ProcessProtocol(d, self._disconnected)
        pT = reactor.spawnProcess(pProtocol, sys.executable, args)
        yield d
        # Now connect to it via the UNIX socket
        dest = endpoints.UNIXClientEndpoint(reactor, self.address)
        self.aP = yield endpoints.connectProtocol(dest, amp.AMP())
        self.pList.append(self.aP)

    def _ditchClass(self):
        self.pList.remove(self.aP)

    def _disconnected(self):
        if getattr(self, '_ignoreDisconnection', False):
            return
        self._ditchClass()
        possibleResignator = getattr(self, 'resignator', None)
        if callable(possibleResignator):
            possibleResignator()
    
    @defer.inlineCallbacks
    def _assembleChunkedResult(self, ID):
        pickleString = ""
        moreLeft = True
        while moreLeft:
            chunk, isValid, moreLeft = yield self.aP.callRemote(GetMore, ID=ID)
            if not isValid:
                raise ValueError("Invalid result chunk with ID={}".format(ID))
            pickleString += chunk
        defer.returnValue(p2o(pickleString))

    # Implementation methods
    # -------------------------------------------------------------------------

    def setResignator(self, callableObject):
        self.resignator = callableObject
    
    def run(self, task):
        """
        Sends the task callable, args, kw to the process and returns a
        deferred to the eventual result.

        You must make sure that the local worker is ready for the next
        task before running this with another one.
        """
        def connected(null):
            # Delete my reference to the wait-for-connect deferred so
            # the one-at-a-time check passes
            del self.d
            # Now really run the first task
            return self.run(task)

        def gotResponse(response):
            status = response['status']
            if status == 'i':
                # An iteratable
                ID = response['result']
                deferator = util.Deferator(
                    "Remote iterator ID={}".format(ID),
                    self.aP.callRemote, GetMore, ID=ID)
                task.callback(('i', deferator))
            elif status == 'c':
                # Chunked result
                dResult = self._assembleChunkedResult(response['result'])
                task.callback(('c', dResult))
            elif status in "re":
                task.callback((status, response['result']))
            else:
                raise ValueError("Unknown status {}".format(status))
        
        # Even a process worker is strictly one task at a time, for now
        self.checkReady()
        # Can't do anything until we are connected
        if not self.dConnected.called:
            d = self.d = defer.Deferred()
            d.addCallback(connected)
            self.dConnected.chainDeferred(d)
            return d
        self.task = task
        # Run the task on the subordinate Python interpreter
        # -----------------------------------------------------------
        # Everything is sent to the pserver as pickled keywords
        kw = {'f': o2p(task.f), 'args': o2p(task.args), 'kw': o2p(task.kw)}
        # The remote call
        self.d = self.aP.callRemote(RunTask, **kw)
        self.d.addCallback(gotResponse)
        return self.d

    def _stopper(self):
        self.aP.callRemote(QuitRunning)

    def stop(self):
        self._ignoreDisconnection = True
        return self.deferToStop(self._stopper)

    def crash(self):
        if hasattr(self, 'd'):
            self._stopper()
            if not self.d.called:
                return [self.task]
        return []
