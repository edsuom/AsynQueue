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

import sys, traceback
import cPickle as pickle

from twisted.internet import defer, reactor
from twisted.python.failure import Failure

import errors
from iteration import Deferator, Prefetcherator



def o2p(obj):
    """
    Converts an object into a pickle string or a blank string if an
    empty container.
    """
    if not obj:
        return ""
    return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)

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


class Info(object):
    """
    I provide text info about a call. Construct me with a function
    object and any args and keywords if you want the info to include
    that particular function call, or you can set it (and change it)
    later with L{setCall}.
    """
    __slots__ = ['callTuple']

    def __init__(self, *args, **kw):
        if args:
            self.callTuple = (args[0], args[1:], kw)

    def setCall(func, *args, **kw):
        self.callTuple = func, args, kw
        return self
    
    def getID(self, *args, **kw):
        """
        Returns a unique ID for my current callable or a func-args-kw
        combination you specify.
        """
        if args:
            return hash((args[0], args[1:], kw))
        return hash(self.callTuple)

    def _divider(self, lineList):
        lineList.append(
            "-" * (max([len(x) for x in lineList]) + 1))
        
    def aboutCall(self):
        """
        Returns an informative string describing my function call.
        """
        callTuple = getattr(self, 'callTuple', None)
        if callTuple is None:
            return ""
        func, args, kw = callTuple
        if func.__class__.__name__ == "function":
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
        return text

    def aboutException(self):
        """
        Returns an informative string describing an exception raised from
        my function call.
        """
        stuff = sys.exc_info()
        lineList = ["Exception '{}'".format(stuff[1])]
        callInfo = self.aboutCall()
        if callInfo:
            lineList.append(
                " doing call '{}':".format(callInfo))
        self._divider(lineList)
        lineList.append("".join(traceback.format_tb(stuff[2])))
        del stuff
        return "\n".join(lineList)

    def aboutFailure(self, failureObj):
        """
        Returns an informative string describing a Twisted failure raised
        from my function call. You can use this as an errback.
        """
        lineList = ["Failure '{}'".format(failureObj.getErrorMessage())]
        callInfo = self.aboutCall()
        if callInfo:
            lineList.append(
                " doing call '{}':".format(callInfo))
        self._divider(lineList)
        lineList.append(failureObj.getTraceback(detail='verbose'))
        return "\n".join(lineList)

    def __call__(self, *args):
        if args and isinstance(args[0], Failure):
            return self.aboutFailure(args[0])
        return self.aboutException()


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
            self.dList.remove(d)
            return anything

        d.addBoth(transparentCallback, d)
        if not isinstance(d, defer.Deferred):
            raise TypeError("Object '%s' is not a deferred" % d)
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
        self.running = True

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
        if not hasattr(self, stoppers):
            self.stoppers = []
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
    

class ThreadLooper(object):
    """
    I run function calls in a dedicated thread, returning a deferred
    to each eventual result.

    If the result is an iterable other than one of Python's built-in
    ones, the deferred fires with an instance of L{Deferator}
    instead. Each of its iterations corresponds to an iteration that
    runs in my thread on the underlying iterable.

    My statusResult attribute is a 2-tuple containing the status of
    the last call and its result:

    'e': An exception was raised; the result is a pretty-printed
      traceback string.

    'r': Ran fine, the result is the return value of the call.

    'i': Ran fine, but the result is an iterable other than a standard
      Python one. The result is an instance of L{Deferator}.

    """
    def __init__(self):
        import threading
        self.lock = DeferredLock()
        self.event = threading.Event()
        self.thread = threading.Thread(target=self.loop)
        self.thread.start()

    def loop(self):
        """
        Runs a loop in a dedicated thread that waits for new tasks. The loop
        exits when a C{None} object is supplied as a task.
        """
        while True:
            # Wait here on the threading.Event object
            self.event.wait()
            if self.callTuple is None:
                # Shutdown was requested
                break
            f, args, kw = self.callTuple
            # Ready for another calltuple to be set
            self.event.clear()
            try:
                result = f(*args, **kw)
                # If the task causes the thread to hang, the method
                # call will not reach this point.
            except Exception as e:
                status = 'e'
                result = Info(f, *args, **kw)
            else:
                if Deferator.isIterator(result):
                    # An iterator
                    pf = Prefetcherator()
                    if pf.setIterator(result):
                        # OK, we can iterate this
                        status = 'i'
                        result = Deferator(
                            repr(result),
                            self.deferToThread, pf.getNext, doNext=True)
                    else:
                        status = 'e'
                        result = "Failed to iterate for call {}".format(
                            callInfo(f, *args, **kw))
                else:
                    # Not an iterator; we already have our result
                    status = 'r'
            reactor.callFromThread(self.d.callback, (status, result))
        # Broken out of loop, the thread now dies

    def call(self, f, *args, **kw):
        """
        Runs the supplied callable function with any args and keywords in
        a dedicated thread, returning a deferred that fires with a
        status/result tuple.

        Calls are done in the order received, unless you set doNext=True.
        
        """
        def threadReady(null):
            self.callTuple = f, args, kw
            self.event.set()
            self.d = defer.Deferred().addCallback(threadDone)
            return self.d

        def threadDone(statusResult):
            self.lock.release()
            return statusResult

        if kw.pop('doNext', False):
            d = self.lock.acquireNext()
        else:
            d = self.lock.acquire()
        return d.addCallback(threadReady)
    
    def deferToThread(self, f, *args, **kw):
        """
        My single-threaded, queued, doNext-able, Deferator-able answer to
        Twisted's deferToThread.

        If you expect a deferred iterator as your result (an instance
        of L{Deferator}), supply an IConsumer implementor via the
        consumer keyword. Each iteration will be written to it, and
        the deferred will fire when the iterations are
        done. Otherwise, the deferred will fire with an
        L{iteration.IterationProducer} and you will have to register
        with and run it yourself.
        
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
        self.lock.addStopper(self.thread.join)
        return self.lock.stop()

