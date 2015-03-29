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

import errors, util, iteration
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
    cQualified = ['thread', 'local']

    def __init__(self, series=[], profiler=None):
        self.tasks = []
        self.iQualified = series
        self.profiler = profiler
        self.t = util.ThreadLooper()

    def setResignator(self, callableObject):
        self.t.dLock.addStopper(callableObject)

    def run(self, task):
        """
        Returns a deferred that fires only after the threaded call is
        done. I do basic FIFO queuing of calls to this method, but
        priority queuing is above my paygrade and you'd best honor my
        deferred and let someone like L{tasks.WorkerManager} only call
        this method when I say I'm ready.

        One simple thing I *will* do is apply the doNext keyword to
        any task with the highest priority, -20 or lower (for a
        L{base.TaskQueue.call} with its own *doNext* keyword set). If
        you call this method one task at a time like you're supposed
        to, even that won't make a difference. But, why not.
        """
        def done(result):
            if task in self.tasks:
                self.tasks.remove(task)
            task.d.callback(result)
        
        self.tasks.append(task)
        f, args, kw = task.callTuple
        if task.priority <= -20:
            kw['doNext'] = True
        # TODO: Have thread run with self.profiler.runcall if profiler present
        return self.t.call(f, *args, **kw).addCallback(done)
    
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
        self.t.stop()
        return self.tasks


class AsyncWorker(object):
    """
    I implement an L{IWorker} that runs tasks in the Twisted main
    loop, one task at a time but in a well-behaved non-blocking
    manner. If the task callable doesn't return a deferred, it better
    get its work done fast.

    You can supply a series keyword containing a list of one or more
    task series that I am qualified to handle.

    Might be useful where you want the benefits of priority queueing
    without leaving the Twisted mindset even for a moment.
    
    """
    implements(IWorker)
    cQualified = ['async', 'local']
    
    def __init__(self, series=[], profiler=None):
        self.iQualified = series
        self.profiler = profiler
        self.info = util.Info()
        self.dLock = util.DeferredLock()

    def setResignator(self, callableObject):
        self.dLock.addStopper(callableObject)

    def run(self, task):
        def ready(null):
            kw.pop('doNext', None)
            # THOU SHALT NOT BLOCK!
            return defer.maybeDeferred(
                f, *args, **kw).addCallbacks(done, oops)

        def done(result):
            if iteration.Deferator.isIterator(result):
                status = 'i'
                if consumer:
                    result = iteration.consumeIterations(result, consumer)
                else:
                    result = iteration.iteratorToProducer(iterator)
            else:
                status = 'r'
            # Hangs if release is done after the task callback
            self.dLock.release()
            task.callback((status, result))

        def oops(failureObj):
            text = self.info.setCall(f, args, kw).aboutFailure(failureObj)
            task.callback(('e', text))

        f, args, kw = task.callTuple
        if self.profiler:
            args = [f] + list(args)
            f = self.profiler.runcall
        consumer = kw.pop('consumer', None)
        return self.dLock.acquire().addCallback(ready)
    
    def stop(self):
        return self.dLock.stop()

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

    When running tasks via me, don't assume that you can just call
    blocking code because it's done remotely. The AMP server on the
    other end runs under Twisted, too, and the result of the call may
    be a deferred. If the call is a blocking one, set the *thread*
    keyword C{True} for it and it will run via an instance of
    L{util.ThreadLoop}.
    
    """
    implements(IWorker)
    cQualified = ['process', 'amp']
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
            if childFD == 1 and data.strip() == 'OK':
                self.d.callback(None)
        def childConnectionLost(self, childFD):
            self.disconnectCallback()
        def processExited(self, reason):
            self.disconnectCallback()
        def processEnded(self, reason):
            self.disconnectCallback()
    
    def __init__(self, reconnect=False, series=[], profiler=None):
        self.iQualified = series
        self.profiler = profiler
        self.dLock = util.DeferredLock()
        self.dLock.acquire()
        self.reconnect = reconnect
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
        self.dLock.release()
        
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
    def assembleChunkedResult(self, ID):
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
        """
        self.tasks.append(task)
        doNext = task.kw.pop('doNext', False)
        if doNext:
            yield self.dLock.acquireNext()
        else:
            yield self.dLock.acquire()
        # Run the task on the subordinate Python interpreter
        # TODO: Have the subordinate run with its own version of
        # self.profiler.runcall if profiler present. It will need to
        # return its own Stats object when we call QuitRunning
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
            deferator = iteration.Deferator(
                "Remote iterator ID={}".format(ID),
                self.aP.callRemote, GetMore, ID=ID)
            task.callback(('i', deferator))
        elif status == 'c':
            # Chunked result, which will take a while
            dResult = yield self.assembleChunkedResult(response['result'])
            task.callback(('c', dResult))
        elif status in "re":
            task.callback((status, response['result']))
        else:
            raise ValueError("Unknown status {}".format(status))
        # Task is now done, one way or another
        self.tasks.remove(task)

    def stop(self):
        self._ignoreDisconnection = True
        self.dLock.addStopper(self._stopper)
        return self.dLock.stop()

    def crash(self):
        if self.aP in self.pList:
            self._stopper()
        return self.tasks
