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
Miscellaneous useful stuff.
"""

import os, signal, sys, traceback
import cPickle as pickle
import cProfile as profile

from twisted.internet import defer, reactor
from twisted.python.failure import Failure

import errors, iteration


def o2p(obj):
    """
    Converts an object into a pickle string or a blank string if an
    empty container.
    """
    if isinstance(obj, (list, tuple, dict)) and not obj:
        return ""
    return pickle.dumps(obj)#, pickle.HIGHEST_PROTOCOL)

def p2o(pickledString, defaultObj=None):
    """
    Converts a pickle string into its represented object, or into the
    default object you specify if it's a blank string.

    Note that a reference to the default object itself will be
    returned, not a copy of it. So make sure you only supply an empty
    Python primitives, e.g., "[]".
    """
    if not pickledString:
        return defaultObj
    return pickle.loads(pickledString)

@defer.inlineCallbacks
def killProcess(pid):
    """
    Kills the process with the supplied PID, returning a deferred that
    fires when it's no longer running. The return value is C{True} if
    the process was alive and had to be killed, C{False} if it was
    already dead.
    """
    def callIfVirgin():
        if not d.called:
            d.callback(None)
    
    pidString = str(pid)
    pp = ProcessProtocol()
    args = ("/bin/ps", '-p', pidString, '--no-headers')
    pt = reactor.spawnProcess(pp, args[0], args)
    stdout = yield pp.waitUntilReady()
    pt.loseConnection()
    if pidString in stdout:
        os.kill(pid, signal.SIGTERM)
        result = True
    result = False
    defer.returnValue(result)


def testFunction(x):
    return 2*x

class TestStuff(object):
    @staticmethod
    def divide(x, y):
        return x/y
    def accumulate(self, y):
        if not hasattr(self, 'x'):
            self.x = 0
        self.x += y
        return self.x
    def setStuff(self, N1, N2):
        self.stuff = ["x"*N1] * N2
        return self
    def stufferator(self):
        for chunk in self.stuff:
            yield chunk
    def blockingTask(self, x, delay):
        import time
        time.sleep(delay)
        return 2*x

    
class CallProfiler(profile.Profile):
    def __init__(self, filename):
        self.filename = filename
        super(CallProfiler, self).__init__()

    def shutdown(self):
        self.dump_stats(self.filename)
        self.disable()
        

class Info(object):
    """
    I provide text (picklable) info about a call. Construct me with a
    function object and any args and keywords if you want the info to
    include that particular function call, or you can set it (and
    change it) later with L{setCall}.
    """
    def __init__(self, remember=False):
        if remember:
            self.pastInfo = {}

    def setCall(self, *metaArgs):
        """
        Sets my current f-args-kw tuple, returning a reference to myself
        to allow easy method chaining.

        The function 'f' must be an actual callable object if you want
        to use L{getWireVersion}. Otherwise it can also be a string
        depicting a callable.

        You can specify args with a second argument (as a list or
        tuple), and kw with a third argument (as a dict).
        """
        f = metaArgs[0]
        args = metaArgs[1] if len(metaArgs) > 1 else []
        kw = metaArgs[2] if len(metaArgs) > 2 else {}
        if hasattr(self, 'currentID'):
            del self.currentID
        self.callTuple = f, args, kw
        self.getID()
        return self
    
    def getID(self):
        """
        Returns a unique ID for my current callable.
        """
        def hashFAK(fak):
            fak[1] = tuple(fak[1])
            fak[2] = (
                tuple(fak[2].keys()),
                tuple(fak[2].values())) if fak[2] else None
            return hash(tuple(fak))

        if hasattr(self, 'currentID'):
            return self.currentID
        if hasattr(self, 'callTuple'):
            ID = hashFAK(list(self.callTuple))
            if hasattr(self, 'pastInfo'):
                self.pastInfo[ID] = {'callTuple': self.callTuple}
        else:
            ID = None
        self.currentID = ID
        return ID

    def forgetID(self, ID):
        if ID in getattr(self, 'pastInfo', {}):
            del self.pastInfo['ID']

    def getInfo(self, ID, name):
        """
        If the supplied name is 'callTuple', returns the f-args-kw tuple
        for my current callable. The value of ID is ignored in such
        case.

        Otherwise, returns the named information attribute for the
        previous call identified with the supplied ID.
        """
        def getCallTuple():
            return getattr(self, 'callTuple', None)
        
        if hasattr(self, 'pastInfo'):
            if ID is None and name == 'callTuple':
                return getCallTuple()
            return self.pastInfo.get(ID, {}).get(name, None)
        if name == 'callTuple':
            return getCallTuple()
        return None
    
    def saveInfo(self, name, text, ID=None):
        if ID is None:
            ID = self.getID()
        if hasattr(self, 'pastInfo'):
            self.pastInfo.setdefault(ID, {})[name] = text
        return text

    def getWireVersion(self, ID=None):
        """
        For my current callable or a previous one identified by ID,
        returns a 2-tuple suitable for sending to a process worker via
        pickle.

        The first element: a pickled object of which func is a method,
        or a string of a global-module importable object containing
        that method, or C{None} if the function is standalone.

        The second element: a string of a callable attribute of the
        object, or a pickled callable itself, or C{None} if nothing
        works.
        """
        def isStr(func):
            return isinstance(func, (str, unicode))
        
        def splitAttr(x):
            parts = x.split(".")
            if len(parts) > 1:
                return ".".join(parts[:-1]), parts[-1]
            return None, x

        def tryReflect():
            np, fn = splitAttr(func)
            try:
                reflect.namedObject(np)
            except:
                return None, None
            return np, fn

        def tryAsMethod():
            """
            See if the function is a method of an object we can pickle,
            returning the pickled object and method name if so.
            """
            parentObj = getattr(func, 'im_self', None)
            if not parentObj:
                return
            # It's a method of an object we may be able to pickle
            # and send
            try:
                np = util.o2p(parentObj)
                util.p2o(np)
            except:
                # Couldn't pickle and unpickle it
                return
            else:
                # We will call with the method's attribute name
                return np, func.__func__.__name__

        def tryFQN():
            """
            See if the function can be defined as a fully qualified name (FQN)
            that can be imported via L{reflect.namedObject}, returning
            the FQN name of its parent object if it's a method along
            with the method name as a string, or C{None} for the
            parent and the FQN of the function itself, if that works
            instead.
            """
            try:
                fqn = reflect.fullyQualifiedName(func)
                reflect.namedObject(fqn)
            except:
                # TODO
                pass
            return splitAttr(fqn)

        if ID:
            pastInfo = self.getInfo(ID, 'wireVersion')
            if pastInfo:
                return pastInfo
        callTuple = self.getInfo(ID, 'wireVersion')
        if not callTuple:
            return None, None
        func = callTuple[0]
        if isStr(func):
            result = tryReflect()
        else:
            # Couldn't pickle/unpickle a parent object, try getting a
            # fully-qualified name instead
            result = tryAsMethod()
            if result is None:
                result = tryFQN()
        
    def _divider(self, lineList):
        lineList.append(
            "-" * (max([len(x) for x in lineList]) + 1))
        
    def aboutCall(self, ID=None):
        """
        Returns an informative string describing my current function call
        or a previous one identified by ID.
        """
        if ID:
            pastInfo = self.getInfo(ID, 'aboutCall')
            if pastInfo:
                return pastInfo
        callTuple = self.getInfo(ID, 'callTuple')
        if not callTuple:
            return ""
        func, args, kw = callTuple
        if isinstance(func, (str, unicode)):
            text = func
        elif func.__class__.__name__ == "function":
            text = func.__name__
        elif callable(func):
            text = "{}.{}".format(func.__class__.__name__, func.__name__)
        else:
            try:
                func = str(func)
            except:
                func = repr(func)
            text = "{}[Not Callable!]".format(func)
        text += "("
        if args:
            text += ", ".join([str(x) for x in args])
        for name, value in kw.iteritems():
            text += ", {}={}".format(name, value)
        text += ")"
        return self.saveInfo('aboutCall', text, ID)
    
    def aboutException(self, ID=None):
        """
        Returns an informative string describing an exception raised from
        my function call or a previous one identified by ID.
        """
        if ID:
            pastInfo = self.getInfo(ID, 'aboutException')
            if pastInfo:
                return pastInfo
        stuff = sys.exc_info()
        lineList = ["Exception '{}'".format(stuff[1])]
        callInfo = self.aboutCall()
        if callInfo:
            lineList.append(
                " doing call '{}':".format(callInfo))
        self._divider(lineList)
        lineList.append("".join(traceback.format_tb(stuff[2])))
        del stuff
        text = "\n".join(lineList)
        return self.saveInfo('aboutException', text, ID)

    def aboutFailure(self, failureObj, ID=None):
        """
        Returns an informative string describing a Twisted failure raised
        from my function call or a previous one identified by ID. You
        can use this as an errback.
        """
        if ID:
            pastInfo = self.getInfo(ID, 'aboutFailure')
            if pastInfo:
                return pastInfo
        lineList = ["Failure '{}'".format(failureObj.getErrorMessage())]
        callInfo = self.aboutCall()
        if callInfo:
            lineList.append(
                " doing call '{}':".format(callInfo))
        self._divider(lineList)
        lineList.append(failureObj.getTraceback(detail='verbose'))
        text = "\n".join(lineList)
        return self.saveInfo('aboutFailure', text, ID)


class DeferredTracker(object):
    """
    I allow you to track and wait for deferreds without actually having
    received a reference to them.
    """
    def __init__(self):
        self.dList = []
    
    def put(self, d):
        """
        Put another deferred in the tracker.
        """
        def transparentCallback(anything):
            if d in self.dList:
                self.dList.remove(d)
            return anything

        d.addBoth(transparentCallback)
        if not isinstance(d, defer.Deferred):
            raise TypeError("Object {} is not a deferred".format(repr(d)))
        self.dList.append(d)
        return d

    def deferToAll(self):
        """
        Return a deferred that tracks all active deferreds that haven't
        yet fired. When the tracked deferreds fire, the returned
        deferred fires, too.
        """
        if self.dList:
            d = defer.DeferredList(self.dList)
            self.dList = []
        elif hasattr(self, 'd_WFA') and not self.d_WFA.called():
            d = defer.Deferred()
            self.d_WFA.chainDeferred(d)
        else:
            d = defer.succeed(None)
        return d

    def deferToLast(self):
        """
        Return a deferred that tracks the deferred that was most recently put
        in the tracker. When the tracked deferred fires, the returned deferred
        fires, too.
        """
        if self.dList:
            d = defer.Deferred()
            self.dList.pop().chainDeferred(d)
        elif hasattr(self, 'd_WFL') and not self.d_WFL.called():
            d = defer.Deferred()
            self.d_WFL.chainDeferred(d)
        else:
            d = defer.succeed(None)
        return d


class DeferredLock(defer.DeferredLock):
    """
    I am a modified form of L{defer.DeferredLock lock that lets you
    shut things down when you get the lock.

    Raises an exception if you try to acquire the lock after a
    shutdown has been initated.

    """
    def __init__(self):
        self.stoppers = []
        self.running = True
        super(DeferredLock, self).__init__()

    def acquireNext(self):
        """
        Like L{defer.DeferredLock.acquire} except cuts ahead of everyone
        else in the waiting list and gets the next lock (unless
        someone else cuts ahead again, with another call of this
        method).
        """
        if not self.running:
            raise errors.QueueRunError
        d = defer.Deferred(canceller=self._cancelAcquire)
        if self.locked:
            self.waiting.insert(0, d)
        else:
            self.locked = True
            d.callback(self)
        return d

    def acquire(self):
        if not self.running:
            raise errors.QueueRunError
        return super(DeferredLock, self).acquire()
    
    def addStopper(self, f, *args, **kw):
        """
        Add a callable (along with any args and kw) to be run when
        shutting things down. The callable may return a deferred, and
        more than one can be added. They will be called, and their
        result awaited, in the order received.

        """
        self.stoppers.append([f, args, kw])
    
    def stop(self):
        """
        Shut things down, when the waiting list empties.
        """
        @defer.inlineCallbacks
        def runStoppers(me):
            while self.stoppers:
                f, args, kw = self.stoppers.pop(0)
                yield defer.maybeDeferred(f, *args, **kw)
            me.release()
                
        self.running = False
        return super(DeferredLock, self).acquire().addCallback(runStoppers)
    

class CallRunner(object):
    """
    Call me with a callTuple to get a 2-tuple containing the status of
    the call and its result:

    'e': An exception was raised; the result is a pretty-printed
         traceback string.

    'r': Ran fine, the result is the return value of the call.

    'i': Ran fine, but the result is an iterable other than a standard
         Python one. The result is an instance of
         L{iteration.Deferator}.

    If the result is an iterable other than one of Python's built-in
    ones, the deferred fires with an instance of
    L{iteration.Deferator} instead. Each of its iterations corresponds
    to an iteration that runs on the underlying iterable, inside a
    callWrapper if you supply one to my constructor.
    """
    def __init__(self, callWrapper=None):
        self.info = Info()
        self.callWrapper = callWrapper

    def __call__(self, callTuple):
        f, args, kw = callTuple
        try:
            result = f(*args, **kw)
            # If the task causes the thread to hang, the method
            # call will not reach this point.
        except Exception as e:
            return ('e', self.info.setCall(f, args, kw).aboutException())
        if iteration.Deferator.isIterator(result):
            # An iterator
            pf = iteration.Prefetcherator()
            if pf.setup(result):
                # OK, we can iterate this
                if self.callWrapper:
                    # With the getNext call wrapped in another function
                    return ('i', iteration.Deferator(
                        repr(result), self.callWrapper, pf.getNext))
                # With the naked getNext call
                return ('i', iteration.Deferator(
                    repr(result), pf.getNext))
            # An iterator, but not a proper one
            return ('e', "Failed to iterate for call {}".format(
                self.info.setCall(f, args, kw).aboutCall()))
        # Not an iterator; we already have our result
        return ('r', result)

        
class ThreadLooper(object):
    """
    I run function calls in a dedicated thread, returning a deferred
    to each eventual result, a 2-tuple containing the status of the
    last call and its result according to the format of L{CallRunner}.

    If the result is an iterable other than one of Python's built-in
    ones, the deferred fires with an instance of
    L{iteration.Deferator} instead. Each of its iterations corresponds
    to an iteration that runs in my thread on the underlying iterable.
    """
    # My default wait timeout is one minute: This is just how long the
    # thread loop waits before checking for a pending deferred and
    # firing it with a timeout error. Otherwise, it simply waits
    # another minute, and it can do that forever with no problem.
    timeout = 60
    
    def __init__(self):
        import threading
        # Just a simple attribute to indicate if the thread loop is
        # running, mostly for unit testing
        self.threadRunning = True
        # Tools
        self.runner = CallRunner(self.deferToThread)
        self.dLock = DeferredLock()
        self.event = threading.Event()
        self.thread = threading.Thread(target=self.loop)
        self.thread.start()
        
    def loop(self):
        """
        Runs a loop in a dedicated thread that waits for new tasks. The loop
        exits when a C{None} object is supplied as a task.
        """
        def callback(status, result):
            reactor.callFromThread(self.d.callback, (status, result))
        
        self.threadRunning = True
        while True:
            # Wait here for my main-thread caller to release me for
            # another call
            self.event.wait(self.timeout)
            # For Python 2.7 and above, we could have just done
            # if not self.event.wait(...):
            if not self.event.isSet():
                # Timed out waiting for the next call. If there indeed
                # was one, we need to let the caller know. That
                # shouldn't ever happen, though.
                if hasattr(self, 'd') and not self.d.called:
                    callback('e', "Thread timed out waiting for this call!")
                continue
            if self.callTuple is None:
                # Shutdown was requested
                break
            status, result = self.runner(self.callTuple)
            
            # We are about to call back the shared deferred, so clear
            # the event to force me to wait for the next call at the
            # top of the loop. The main thread will not set the event
            # again until the callback is done, so this is safe.
            self.event.clear()
            # OK, now call the shared deferred
            callback(status, result)
        # Broken out of loop, the thread now dies
        self.threadRunning = False

    def call(self, f, *args, **kw):
        """
        Runs the supplied callable function with any args and keywords in
        a dedicated thread, returning a deferred that fires with a
        status/result tuple.

        Calls are done in the order received, unless you set doNext=True.
        
        """
        def threadReady(null):
            self.callTuple = f, args, kw
            self.d = defer.Deferred().addCallback(threadDone)
            # The callTuple is set for this call along with the
            # deferred to be called back with its result, so release
            # the thread to work on it.
            self.event.set()
            return self.d

        def threadDone(statusResult):
            # The deferred lock is released after the call is done so
            # that another call can proceed. This is NOT the same as
            # the event used as a threading lock. It keeps the main
            # thread from setting that event before the thread loop is
            # read for that.
            self.dLock.release()
            return statusResult

        if kw.pop('doNext', False):
            d = self.dLock.acquireNext()
        else:
            d = self.dLock.acquire()
        return d.addCallback(threadReady)
    
    def deferToThread(self, f, *args, **kw):
        """
        My single-threaded, queued, doNext-able, Deferator-able answer to
        Twisted's deferToThread.

        If you expect a deferred iterator as your result (an instance
        of L{iteration.Deferator}), supply an IConsumer implementor
        via the consumer keyword. Each iteration will be written to
        it, and the deferred will fire when the iterations are
        done. Otherwise, the deferred will fire with an
        L{iteration.iteration.IterationProducer} and you will have to
        register with and run it yourself.
        
        """
        def done(statusResult):
            status, result = statusResult
            if status == 'e':
                return Failure(errors.WorkerError(result))
            if status == 'i':
                ip = iteration.IterationProducer(result)
                if consumer:
                    ip.registerConsumer(consumer)
                    return ip.run()
                return ip
            return result
        
        consumer = kw.pop('consumer', None)
        return self.call(f, *args, **kw).addCallback(done)

    def stop(self):
        """
        The returned deferred fires when the task loop has ended and its
        thread terminated.
        """
        # Tell the thread to quit with a null task
        self.callTuple = None
        self.event.set()
        # Now stop the lock
        self.dLock.addStopper(self.thread.join)
        return self.dLock.stop()


class ProcessUniverse(object):
    """
    Each process for a L{workers.ProcessWorker} lives in one of these.
    """
    def __init__(self):
        self.runner = CallRunner()
    
    def loop(self, connection):
        """
        Runs a loop in a dedicated process that waits for new tasks. The
        loop exits when a C{None} object is supplied as a task.
        """
        while True:
            # Wait here for the next task
            callSpec = connection.recv()
            if callSpec is None:
                # Termination task, no reply expected; just exit the
                # loop
                break
            connection.send(self.runner(callSpec))
        # Broken out of loop, ready for the process to end
        connection.close()

        
class ProcessProtocol(object):
    """
    I am a simple protocol for a master Python interpreter to spawn
    and communicate with a subordinate Python. This protocol is used
    by the master (client).
    """
    def __init__(self, disconnectCallback=None):
        self.d = defer.Deferred()
        self.disconnectCallback = disconnectCallback

    def callback(self, text):
        if callable(self.disconnectCallback):
            self.disconnectCallback(text)
        
    def waitUntilReady(self):
        return self.d

    def makeConnection(self, process):
        pass

    def childDataReceived(self, childFD, data):
        data = data.strip()
        if childFD == 1:
            self.d.callback(data)
        elif childFD == 2:
            self.callback("ERROR: {}".format(data))

    def childConnectionLost(self, childFD):
        self.callback("CONNECTION LOST!")

    def processExited(self, reason):
        self.callback(reason)

    def processEnded(self, reason):
        self.callback(reason)

