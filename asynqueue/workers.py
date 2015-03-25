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
        self.t.lock.addStopper(callableObject)

    def run(self, task):
        f, args, kw = task.callTuple
        return self.t.call(f, *args, **kw).addCallback(task.d.callback)
    
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
        self.info = util.Info()
        self.lock = util.DeferredLock()

    def setResignator(self, callableObject):
        self.lock.addStopper(callableObject)

    def run(self, task):
        def ready(lock):
            lock.release()
            # This must not block!
            return defer.maybeDeferred(
                f, *args, **kw).addCallbacks(done, oops)

        def done(result):
            task.callback((True, result))

        def oops(failureObj):
            text = self.info.setCall(
                f, *args, **kw).aboutFailure(failureObj)
            task.callback((False, text))

        f, args, kw = task.callTuple
        return self.lock.acquire().addCallback(ready)
    
    def stop(self):
        return self.lock.stop()

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
        def __init__(self, disconnectCallback):
            self.d = d
            self.disconnectCallback = disconnectCallback
        def waitUntilReady(self):
            return self.d
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
        self.lock = util.DeferredLock()
        self.lock.acquire()
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
        # Start the server and make the connection, releasing the lock
        # when ready to accept tasks
        self._spawnAndConnect().addCallback(self._releaseLock)

    def _spawnAndConnect(self):
        def ready(null):
            # Now connect to the pserver via the UNIX socket
            dest = endpoints.UNIXClientEndpoint(reactor, self.address)
            return endpoints.connectProtocol(
                dest, amp.AMP()).addCallback(connected)

        def connected(aP):
            self.aP = aP
            self.tasks = []
            self.pList.append(self.aP)

        # Spawn the pserver and "wait" for it to indicate it's OK
        args = [sys.executable, "-m", "asynqueue.pserver", self.address]
        pP = self.ProcessProtocol(d, self._disconnected)
        reactor.spawnProcess(pP, sys.executable, args)
        return pP.waitUntilReady().addCallback(ready)

    def _releaseLock(self, *args):
        self.lock.release()
        
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
        moreLeft = True
        pickleString = ""
        while moreLeft:
            stuff = yield self.aP.callRemote(GetMore, ID=ID)
            chunk, isValid, moreLeft = stuff
            if not isValid:
                raise ValueError(
                    "Invalid result chunk with ID={}".format(ID))
            pickleString += chunk
        defer.returnValue(p2o(pickleString))

    def _stopper(self):
        self._ditchClass()
        return self.aP.callRemote(QuitRunning)

    # Implementation methods
    # -------------------------------------------------------------------------

    def setResignator(self, callableObject):
        self.resignator = callableObject
    
    @defer.inlineCallbacks
    def run(self, task):
        """
        Sends the task callable, args, kw to the process and returns a
        deferred to the eventual result.

        You must make sure that the local worker is ready for the next
        task before running this with another one.
        """
        def ready(null):

        def gotResponse(response):
        
        self.tasks.append(task)
        yield self.lock.acquire()
        # Run the task on the subordinate Python interpreter
        #-----------------------------------------------------------
        # Everything is sent to the pserver as pickled keywords
        kw = {'f': o2p(task.f),
              'args': o2p(task.args),
              'kw': o2p(task.kw)
          }
        # The heart of the matter
        response = yield self.aP.callRemote(RunTask, **kw)
        #-----------------------------------------------------------
        # At this point, we can permit another remote call to get
        # going for a separate task.
        self._releaseLock()
        # Process the response. No lock problems even if that
        # involves further remote calls, i.e., GetMore
        status = response['status']
        if status == 'i':
            # An iteratable
            ID = response['result']
            deferator = util.Deferator(
                "Remote iterator ID={}".format(ID),
                self.aP.callRemote, GetMore, ID=ID)
            task.callback(('i', deferator))
        elif status == 'c':
            # Chunked result, which will take a while
            dResult = yield self._assembleChunkedResult(response['result'])
            task.callback(('c', dResult))
        elif status in "re":
            task.callback((status, response['result']))
        else:
            raise ValueError("Unknown status {}".format(status))
        # Task is now done, one way or another
        self.tasks.remove(task)

    def stop(self):
        self._ignoreDisconnection = True
        self.lock.addStopper(self._stopper)
        return self.lock.stop()

    def crash(self):
        if self.aP in self.pList:
            self._stopper()
        return self.tasks
