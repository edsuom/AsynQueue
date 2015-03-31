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
import sys, os, os.path, tempfile, shutil

from zope.interface import implements
from twisted.python import failure, reflect
from twisted.internet import defer, reactor, endpoints
from twisted.protocols import amp

from interfaces import IWorker
import errors, util, iteration, pserver


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
        to, even that won't make a difference, except that it will cut
        in front of any existing call with *doNext* set. So use
        judiciously.
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
            # THOU SHALT NOT BLOCK!
            return defer.maybeDeferred(
                f, *args, **kw).addCallbacks(done, oops)

        def done(result):
            if iteration.Deferator.isIterator(result):
                pf = iteration.Prefetcherator()
                if pf.setup(result):
                    # OK, we can iterate this
                    status = 'i'
                    result = iteration.Deferator(repr(result), pf.getNext)
                else:
                    status = 'e'
                    result = "Failed to iterate for call {}".format(
                        self.info.setCall(f, args, kw).aboutCall())
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
        if kw.pop('doNext', False) or task.priority <= -20:
            d = self.dLock.acquireNext()
        else:
            d = self.dLock.acquire()
        return d.addCallback(ready)
    
    def stop(self):
        return self.dLock.stop()

    def crash(self):
        """
        There's no point to implementing this because the Twisted main
        loop will block along with any task you give this worker.
        """

        
class ProcessWorker(object):
    """
    I implement an L{IWorker} that runs tasks in a dedicated worker
    process.

    You can also supply a series keyword containing a list of one or
    more task series that I am qualified to handle.

    All my tasks are run in a single instance of
    L{util.ThreadLoop}. Block away!
    """
    pollInterval = 0.01
    backOff = 1.04
    
    implements(IWorker)
    cQualified = ['process', 'local']

    def __init__(self, series=[], profiler=None):
        self.tasks = []
        self.iQualified = series
        self.profiler = profiler
        self.dLock = util.DeferredLock()
        # Multiprocessing with (Gasp! Twisted heresy!) standard lib Python
        import multiprocessing as mp
        self.cMain, cProcess = mp.Pipe()
        pu = util.ProcessUniverse()
        self.process = mp.Process(target=pu.loop, args=(cProcess,))
        self.process.start()

    def _killProcess(self):
        self.cMain.close()
        self.process.terminate()
        
    # Implementation methods
    # -------------------------------------------------------------------------
        
    def setResignator(self, callableObject):
        self.dLock.addStopper(callableObject)

    @defer.inlineCallbacks
    def run(self, task):
        """
        Sends the task callable and args, kw to the process (must all be
        picklable) and polls the interprocess connection for a result,
        with exponential backoff.
        """
        if task is None:
            # A termination task, do after pending tasks are done
            yield self.dLock.acquire()
            self.cMain.send(None)
            # Wait (a very short amount of time) for the process loop
            # to exit
            self.process.join()
        else:
            self.tasks.append(task)
            if task.priority > -20:
                # Wait in line behind all pending tasks
                yield self.dLock.acquire()
            else:
                # This is high priority; cut in line just behind the
                # current pending task
                yield self.dLock.acquireNext()
            # Our turn!
            self.cMain.send(task.callTuple)
            # "wait" here (in Twisted-friendly fashion) for a response
            # from the process
            delay = self.pollInterval
            while True:
                if self.cMain.poll():
                    # Got a result!
                    if task in self.tasks:
                        self.tasks.remove(task)
                    task.callback(self.cMain.recv())
                    break
                else:
                    # Not yet, check again after the poll interval,
                    # which increases exponentially so that each
                    # incremental delay is somewhat proportional to
                    # the amount of time spent waiting thus far.
                    yield iteration.deferToDelay(delay)
                    delay *= self.backOff
        # Ready for another task or shutdown
        self.dLock.release()
    
    def stop(self):
        """
        The returned deferred fires when the task loop has ended and its
        process terminated.
        """
        def terminationTaskDone(null):
            self._killProcess()
            return self.dLock.stop()            
        return self.run(None).addCallback(terminationTaskDone)

    def crash(self):
        self._killProcess()
        return self.tasks

                    
class SocketWorker(object):
    """
    I implement an L{IWorker} that runs tasks in a subordinate or
    remote Python interpreter via Twisted's Asynchronous Messaging
    Protocol.

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
    pList = []
    tempDir = []
    cQualified = ['process', 'network']
    
    def __init__(self, series=[], sFile=None, reconnect=False, profiler=None):
        self.tasks = []
        self.iQualified = series
        self.profiler = profiler
        # TODO: Implement reconnect option?
        self.reconnect = reconnect
        # Acquire lock until subordinate spawning and AMP connection
        self.dLock = util.DeferredLock()
        self._spawnAndConnect(sFile)
        
    def _newSocketFile(self):
        """
        Assigns a unique name to a socket file in a temporary directory
        common to all instances of me, which will be removed with all
        socket files after reactor shutdown. Doesn't actually create
        the socket file; the server does that.
        """
        # The process name
        pName = "worker-{:03d}".format(len(self.pList))
        # A unique temp directory for all instances' socket files
        if self.tempDir:
            tempDir = self.tempDir[0]
        else:
            tempDir = tempfile.mkdtemp()
            reactor.addSystemEventTrigger(
                'after', 'shutdown',
                shutil.rmtree, tempDir, ignore_errors=True)
            self.tempDir.append(tempDir)
        return os.path.join(tempDir, "{}.sock".format(pName))
    
    def _spawnAndConnect(self, sFile=None):
        """
        Spawn a subordinate Python interpreter and connects to it via the
        AMP protocol and a unique UNIX socket address.
        """
        def ready(null):
            # Now connect to the pserver via the UNIX socket
            dest = endpoints.UNIXClientEndpoint(reactor, sFile)
            return endpoints.connectProtocol(
                dest, amp.AMP()).addCallback(connected)

        def connected(ap):
            self.ap = ap
            self.dLock.release()
            self.pList.append(self.ap)

        if sFile is None:
            sFile = self._newSocketFile()
        if hasattr(self, 'ap'):
            # We already have a connection
            return defer.succeed(None)
        self.dLock.acquire()
        # Spawn the pserver and "wait" for it to indicate it's OK
        args = [sys.executable, "-m", "asynqueue.pserver", sFile]
        pp = util.ProcessProtocol(self._disconnected)
        self.pt = reactor.spawnProcess(pp, sys.executable, args)
        self.pid = self.pt.pid
        return pp.waitUntilReady().addCallback(ready)
        
    def _ditchClass(self):
        if hasattr(self, 'ap'):
            if self.ap in self.pList:
                self.pList.remove(self.ap)
            del self.ap

    def _disconnected(self, reason):
        self._ditchClass()
        if getattr(self, '_ignoreDisconnection', False):
            return
        possibleResignator = getattr(self, 'resignator', None)
        if callable(possibleResignator):
            possibleResignator()
    
    @defer.inlineCallbacks
    def assembleChunkedResult(self, ID):
        moreLeft = True
        pickleString = ""
        while moreLeft:
            stuff = yield self.ap.callRemote(pserver.GetMore, ID=ID)
            chunk, isValid, moreLeft = stuff
            if not isValid:
                raise ValueError(
                    "Invalid result chunk with ID={}".format(ID))
            pickleString += chunk
        defer.returnValue(util.p2o(pickleString))

    @defer.inlineCallbacks
    def _stopper(self):
        self._ditchClass()
        if hasattr(self, 'ap'):
            yield self.ap.callRemote(pserver.QuitRunning)
            yield self.ap.transport.loseConnection()
        if hasattr(self, 'pt'):
            yield self.pt.loseConnection()
        if hasattr(self, 'pid'):
            yield util.killProcess(self.pid)

    def _processFunc(self, func):
        """
        Returns a 2-tuple with (1) a pickled object of which func is a
        method, or a string of a global-module importable object
        containing that method, or C{None}, and (2) a string of a
        callable attribute of the object, or a pickled callable
        itself, or C{None} if nothing works.
        """
        def splitAttr(x):
            parts = x.split(".")
            return ".".join(parts[:-1]), parts[-1]
        
        if isinstance(func, (str, unicode)):
            np, fn = splitAttr(func)
            try:
                reflect.namedObject(np)
            except:
                return None, None
            return np, fn
        # Try to define a namespace for the function and then we can
        # call it by name
        parentObj = getattr(func, 'im_self', None)
        if parentObj:
            # It's a method of an object we may be able to pickle and
            # send
            try:
                np = util.o2p(parentObj)
                util.p2o(np)
            except:
                # Couldn't pickle and unpickle it
                pass
            else:
                # We will call with the method's attribute name
                return np, func.__func__.__name__
        # Couldn't pickle/unpickle a parent object, try getting a
        # fully-qualified name instead
        try:
            fqn = reflect.fullyQualifiedName(func)
            reflect.namedObject(fqn)
        except:
            # This didn't work either. Just try pickling the
            # callable and hope for the best
            try:
                fn = util.o2p(func)
            except:
                return None, None
            else:
                return None, fn
        else:
            return splitAttr(fqn)
            
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
        def result(value):
            task.callback((status, value))
            self.tasks.remove(task)

        self.tasks.append(task)
        doNext = task.callTuple[2].pop('doNext', False)
        if doNext:
            yield self.dLock.acquireNext()
        else:
            yield self.dLock.acquire()
        # Run the task on the subordinate Python interpreter
        # TODO: Have the subordinate run with its own version of
        # self.profiler.runcall if profiler present. It will need to
        # return its own Stats object when we call pserver.QuitRunning
        #-----------------------------------------------------------
        kw = {}
        func = task.callTuple[0]
        np, fn = self._processFunc(func)
        if np:
            if getattr(self, 'lastNamespace', None) != np:
                response = yield self.ap.callRemote(pserver.SetNamespace, np=np)
                print "NP-RESPONSE", np, response
                if response['status'] != "OK":
                    fn = None
                    self.lastNamespace = np
        if fn is None:
            response['status'] = 'e'
            response['result'] = \
                "Couldn't find a way to send callable {}".format(repr(func))
        else:
            kw['fn'] = fn
            for k, value in enumerate(task.callTuple[1:]):
                name = pserver.RunTask.arguments[k+1][0]
                kw[name] = value if isinstance(value, str) else util.o2p(value)
            # The heart of the matter
            response = yield self.ap.callRemote(pserver.RunTask, **kw)
        #-----------------------------------------------------------
        # At this point, we can permit another remote call to get
        # going for a separate task.
        self.dLock.release()
        # Process the response. No lock problems even if that
        # involves further remote calls, i.e., pserver.GetMore
        status = response['status']
        if status == 'i':
            # An iteratable
            ID = response['result']
            deferator = iteration.Deferator(
                "Remote iterator ID={}".format(ID),
                self.ap.callRemote, pserver.GetMore, ID=ID)
            result(deferator)
        elif status == 'c':
            # Chunked result, which will take a while
            dResult = yield self.assembleChunkedResult(response['result'])
            result(dResult)
        elif status == 'r':
            result(util.p2o(response['result']))
        elif status == 'n':
            result(None)
        elif status == 'e':
            result(response['result'])
        else:
            raise ValueError("Unknown status {}".format(status))

    def stop(self):
        self._ignoreDisconnection = True
        self.dLock.addStopper(self._stopper)
        return self.dLock.stop()

    def crash(self):
        if hasattr(self, 'ap') and self.ap in self.pList:
            self._stopper()
        return self.tasks
