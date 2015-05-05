# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
#
# Copyright (C) 2006-2007, 2015 by Edwin A. Suominen,
# http://edsuom.com/AsynQueue
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Miscellaneous useful stuff.

L{callAfterDeferred} is a cool little function that looks for a
C{Deferred} as an attribute of some namespace (i.e., object) and does
a call after it fires. L{DeferredTracker} lets you to track and wait
for deferreds without actually having received a reference to
them. L{DeferredLock} lets you shut things down when you get the lock.

L{CallRunner} is used by L{threads.ThreadWorker} and
L{process.ProcessWorker}. You probably won't need to use it yourself,
unless perhaps you come up with an entirely new kind of
L{interfaces.IWorker} implementation.
"""

from time import time
import cPickle as pickle
import cProfile as profile
from contextlib import contextmanager

from twisted.internet import defer
from twisted.python.failure import Failure

import errors, info, iteration


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
    Python primitive, e.g., C{[]}.
    """
    if not pickledString:
        return defaultObj
    return pickle.loads(pickledString)

def callAfterDeferred(namespace, dName, f, *args, **kw):
    """
    Looks for a C{Deferred} I{dName} as an attribute of I{namespace}
    and does the f-args-kw call, chaining its call to the C{Deferred}
    if necessary.

    Note that the original deferred's value is swallowed when it calls
    the new deferred's callback; the original deferred must be for
    signalling readiness only and its return value not relied upon.
    """
    def call(discarded):
        delattr(namespace, dName)
        return defer.maybeDeferred(f, *args, **kw)        
    
    d = getattr(namespace, dName, None)
    if d is None:
        return defer.maybeDeferred(f, *args, **kw)
    if d.called:
        delattr(namespace, dName)
        return defer.maybeDeferred(f, *args, **kw)
    d2 = defer.Deferred().addCallback(call)
    d.chainDeferred(d2)
    return d2


# For Testing
# ----------------------------------------------------------------------------
def testFunction(x):
    """
    I{For testing only.}
    """
    return 2*x
class TestStuff(object):
    """
    I{For testing only.}
    """
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
# ----------------------------------------------------------------------------
    

class DeferredTracker(object):
    """
    I allow you to track and wait for deferreds without actually having
    received a reference to them.
    """
    def __init__(self):
        self.dList = []
    
    def put(self, d):
        """
        Put another C{Deferred} in the tracker.
        """
        def transparentCallback(anything):
            if d in self.dList:
                self.dList.remove(d)
            return anything

        if not isinstance(d, defer.Deferred):
            raise TypeError("Object {} is not a deferred".format(repr(d)))
        d.addBoth(transparentCallback)
        self.dList.append(d)
        return d

    def _sweep(self):
        for d in self.dList:
            if d.called:
                self.dList.remove(d)
        
    def deferToAll(self):
        """
        Return a C{Deferred} that tracks all active deferreds that haven't
        yet fired. When the tracked deferreds fire, the returned
        deferred fires, too.
        """
        # Sweep of already called deferreds is only done when waiting
        # for all unfired ones
        self._sweep()
        return defer.DeferredList(self.dList)

    def deferToLast(self):
        """
        Return a C{Deferred} that tracks the C{Deferred} that was most
        recently put in the tracker. When the tracked deferred fires,
        the returned deferred fires, too.
        """
        def transparentCallback(anything):
            d.callback(None)
            # Any already called deferreds remaining are now removed
            self._sweep()
            return anything

        # The last-added of ALL remaining deferreds is chained to,
        # even if already called
        if self.dList:
            d = defer.Deferred()
            self.dList[-1].addBoth(transparentCallback)
            return d
        return defer.succeed(None)


class DeferredLock(defer.DeferredLock):
    """
    I am a modified form of L{defer.DeferredLock} lock that lets you
    shut things down when you get the lock.

    Raises an exception if you try to acquire the lock after a
    shutdown has been initated.
    """
    def __init__(self):
        self.N_vips = 0
        self.stoppers = []
        self.running = True
        super(DeferredLock, self).__init__()

    @contextmanager
    def context(self, vip=False):
        """
        Usage example, inside a defer.inlineCallbacks function::

          with lock.context() as d:
              # "Wait" for the 
              yield d
              <Do something that requires holding onto the lock>
          <Proceed with the lock released>
        
        """
        yield self.acquire(vip)
        self.release()
        
    def acquire(self, vip=False):
        """
        Like L{defer.DeferredLock.acquire} except with a I{vip}
        option. That lets you cut ahead of everyone in the regular
        waiting list and gets the next lock, after anyone else in the
        VIP line who is waiting from their own call of this method.
        """
        def transparentCallback(result):
            self.N_vips -= 1
            return result
        
        if not self.running:
            raise errors.QueueRunError(
                "Can't acquire from a stopped DeferredLock")
        d = defer.Deferred(canceller=self._cancelAcquire)
        if self.locked:
            if vip:
                d.addCallback(transparentCallback)
                self.waiting.insert(self.N_vips, d)
                self.N_vips += 1
            else:
                self.waiting.append(d)
        else:
            self.locked = True
            d.callback(self)
        return d

    def acquireAndRelease(self, vip=False):
        return self.acquire(vip).addCallback(lambda x: x.release())
        
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
    I'm used by L{threads.ThreadLooper} and
    L{process.ProcessUniverse}.
    """
    def __init__(self, raw=False, callStats=False):
        """
        @param raw: Set C{True} to return raw iterators by default instead
          of doing L{iteration} magic.
        @param callStats: Set C{True} to accumulate a list of
          I{callTimes} for each call. B{Caution:} Can get big with
          lots of calls!
        """
        self.raw = raw
        self.info = info.Info()
        self.callStats = callStats
        if callStats:
            self.callTimes = []

    def __call__(self, callTuple):
        """
        Does the f-args-kw call in I{callTuple} to get a 2-tuple
        containing the status of the call and its result:

        - B{e}: An exception was raised; the result is a
          pretty-printed traceback string, unless I am constructed
          with I{returnFailures} set. Then the result is a C{Failure}
          object.
  
        - B{r}: Ran fine, the result is the return value of the call.
  
        - B{i}: Ran fine, but the result is an iterable other than a
          standard Python one.

        Honors the I{raw} option to return iterators as-is if
        desired. The called function never sees that keyword.
        """
        f, args, kw = callTuple
        raw = kw.pop('raw', None)
        if raw is None:
            raw = self.raw
        try:
            if self.callStats:
                t0 = time()
                result = f(*args, **kw)
                self.callTimes.append(time()-t0)
            else:
                result = f(*args, **kw)
            # If the task causes the thread to hang, the method
            # call will not reach this point.
        except:
            result = self.info.setCall(f, args, kw).aboutException()
            return ('e', result)
        if not raw and iteration.Deferator.isIterator(result):
            return ('i', result)
        return ('r', result)

        

