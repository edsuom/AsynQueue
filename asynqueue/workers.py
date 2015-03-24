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
The worker interface and some implementors.
"""
import subprocess

from zope.interface import implements, invariant, Interface, Attribute
from twisted.python import failure
from twisted.internet import defer, reactor, endpoints
from twisted.protocols import amp

import errors


class IWorker(Interface):
    """
    Provided by worker objects that can have tasks assigned to them for
    processing.

    All worker objects are considered qualified to run tasks of the default
    C{None} series. To indicate that subclasses or subclass instances are
    qualified to run tasks of user-defined series in addition to the default,
    the hashable object that identifies the additional series must be listed in
    the C{cQualified} or C{iQualified} class or instance attributes,
    respectively.
        
    """
    cQualified = Attribute(
        """
        A class-attribute list containing all series for which all instances of
        the subclass are qualified to run tasks.
        """)

    iQualified = Attribute(
        """
        An instance-attribute list containing all series for which the subclass
        instance is qualified to run tasks.
        """)

    def _check_qualifications(ob):
        """
        Qualification attributes must be present as lists.
        """
        for attrName in ('cQualified', 'iQualified'):
            x = getattr(ob, attrName, None)
            if not isinstance(x, list):
                raise errors.InvariantError(ob)
    invariant(_check_qualifications)

    def setResignator(callableObject):
        """
        Registers the supplied I{callableObject} to be called if the
        worker deems it necessary to resign, e.g., a remote connection
        has been lost.
        """

    def run(task):
        """
        Adds the task represented by the specified I{task} object to the list
        of tasks pending for this worker, to be run however and whenever the
        worker sees fit.

        Make sure that any callbacks you add to the task's internal deferred
        object C{task.d} return the callback argument. Otherwise, the result of
        your task will be lost in the callback chain.
        
        @return: A deferred that fires when the worker is ready to be assigned
          another task.

        """

    def stop():
        """
        Attempts to gracefully shut down the worker, returning a deferred that
        fires when the worker is done with all assigned tasks and will not
        cause any errors if the reactor is stopped or its object is deleted.

        The deferred returned by your implementation of this method must not
        fire until B{after} the results of all pending tasks have been
        obtained. Thus the deferred must be chained to each C{task.d} somehow.

        Make sure that any callbacks you add to the task's internal deferred
        object C{task.d} return the callback argument. Otherwise, the result of
        your task will be lost in the callback chain.
        """

    def crash():
        """
        Takes drastic action to shut down the worker, rudely and
        synchronously.

        @return: A list of I{task} objects, one for each task left
          uncompleted. You shouldn't have to call this method if no
          tasks are left pending; the L{stop} method should be enough
          in that case.

        """


class WorkerBase(object):
    """
    Subclass me to get some worker goodness.
    """
    implements(IWorker)
    cQualified = []

    def taskTraceback(self, e):
        import traceback
        lineList = [
            "Exception '{}'".format(str(e)),
            " running task '{}':".format(repr(self.task))]
        lineList.append(
            "-" * (max([len(x) for x in lineList]) + 1))
        lineList.append("".join(traceback.format_tb(e[2])))
        return "\n".join(lineList)

    def checkReady(self):
        """
        Checks if I'm ready for another task and raises an exception if
        not.
        """
        if hasattr(self, 'd') and not self.d.called:
            raise errors.NotReadyError(
                "You shouldn't have called yet. " +\
                "Task Loop not ready to deal with a task now")

    def deferToStop(self, stopper=None):
        """
        Returns a deferred that fires when any pending task is done and
        any no-arg stopper function you supply has run.
        """
        def runStopper(*arg):
            self.dStop = defer.maybeDeferred(stopper)
            return self.dStop
        
        if hasattr(self, 'dStop'):
            # A stop was already initiated...
            if self.dStop.called:
                # ..and it's done
                d = defer.succeed(None)
            else:
                # ...no, it's still pending
                d = defer.Deferred()
                self.dStop.chainDeferred(d)
            return d
        if hasattr(self, 'd') and not self.d.called:
            # No stop yet pending, but there's a task pending (a real
            # one; we wouldn't get this far if it were just a null
            # task for stopping a thread's loop)
            d = defer.Deferred()
            # Run the stopper when the task is done
            if stopper:
                d.addCallback(runStopper)
            self.d.chainDeferred(d)
            return d
        # No task or stop pending, so run the stopper function now, if
        # there is one
        if stopper:
            return runStopper()
        return defer.succeed(None)


class ThreadWorker(WorkerBase):
    """
    I implement an L{IWorker} that runs tasks in a dedicated worker
    thread.

    You can supply a series keyword containing a list of one or more
    task series that I am qualified to handle.
    """
    def __init__(self, series=[]):
        self.iQualified = series
        import threading
        self.event = threading.Event()
        self.thread = threading.Thread(target=self._loop)
        self.thread.start()

    def _loop(self):
        """
        Runs a loop in a dedicated thread that waits for new tasks. The loop
        exits when a C{None} object is supplied as a task.
        """
        while True:
            # Wait here on the threading.Event object
            self.event.wait()
            task = self.task
            if task is None:
                break
            # Ready for the task attribute to be set to another task object
            self.event.clear()
            reactor.callFromThread(self.d.callback, None)
            f, args, kw = task.callTuple
            try:
                result = f(*args, **kw)
                # If the task causes the thread to hang, the method
                # call will not reach this point.
            except Exception as e:
                statusResult = (False, self.taskTraceback(e))
            else:
                statusResult = (True, result)
            reactor.callFromThread(task.callback, statusResult)
        # Broken out of loop, ready for the thread to end
        reactor.callFromThread(self.d.callback, None)
        # The thread now dies

    def setResignator(self, callableObject):
        """
        There's nothing that would make a thread worker resign on its own.
        """

    def run(self, task):
        self.checkReady()
        # Thread workers are strictly one task at a time
        self.task = task
        self.event.set()
        self.d = defer.Deferred()
        return self.d
    
    def stop(self):
        """
        The returned deferred fires when the task loop has ended and its thread
        terminated.
        """
        def stopper():
            self.d = defer.Deferred()
            # Kill the thread when it quits its loop and fires this deferred
            self.d.addCallback(lambda _ : self.thread.join())
            # Tell the thread to quit with a null task
            self.task = None
            self.event.set()
            return self.d
        return self.deferToStop(stopper)

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
        if hasattr(self, 'd') and not self.d.called:
            del self.task
            self.d.callback(None)


class AsyncWorker(WorkerBase):
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
        

class ProcessWorker(WorkerBase):
    """
    I implement an L{IWorker} that runs tasks in a subordinate Python
    interpreter via Twisted's Asynchronous Messaging Protocol.

    You can also supply a series keyword containing a list of one or
    more task series that I am qualified to handle.
    """
    pLists = []

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
        aP = yield endpoints.connectProtocol(dest, amp.AMP())
        self.pList = [pP, pT, aP]
        self.pLists.append(self.pList)

    def _ditchClass(self):
        self.pLists.remove(self.pList)

    def _disconnected(self):
        if getattr(self, '_ignoreDisconnection', False):
            return
        self._ditchClass()
        possibleResignator = getattr(self, 'resignator', None)
        if callable(possibleResignator):
            possibleResignator()
    
    # Implementation methods
    # -------------------------------------------------------------------------

    def setResignator(self, callableObject):
        self.resignator = callableObject

    def run(self, task):
        """
        Sends the task callable, args, kw to the process returns a
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
            failureInfo = response['failureInfo']
            if failureInfo:
                task.callback((False, failureInfo))
            else:
                result = pickle.loads(response['result_pickled'])
                task.callback((True, result))
        
        # Even a process worker is strictly one task at a time, for now
        self.checkReady()
        # Can't do anything until we are connected
        if not self.dConnected.called:
            d = self.d = defer.Deferred()
            d.addCallback(connected)
            self.dConnected.chainDeferred(d)
            return d
        self.task = task
        # Everything is sent to the pserver as pickled keywords
        kw = {'f_pickled': pickle.dumps(task.f, pickle.HIGHEST_PROTOCOL)}
        kw['args_pickled'] = pickle.dumps(
            task.args, pickle.HIGHEST_PROTOCOL) if task.args else ""
        kw['kw_pickled'] = pickle.dumps(
            task.kw, pickle.HIGHEST_PROTOCOL) if task.kw else ""
        # Remotely run the task
        self.d = self.pList[2].callRemote(RunTask, **kw)
        self.d.addCallback(gotResponse)
        return self.d

    def _stopper(self):
        self.pList[2].callRemote(QuitRunning)

    def stop(self):
        self._ignoreDisconnection = True
        return self.deferToStop(self._stopper)

    def crash(self):
        if hasattr(self, 'd'):
            self._stopper()
            if not self.d.called:
                return [self.task]
        return []
