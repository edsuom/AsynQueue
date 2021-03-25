# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
#
# Copyright (C) 2006-2007, 2015, 2019 by Edwin A. Suominen,
# http://edsuom.com/AsynQueue
#
# See edsuom.com for API documentation as well as information about
# Ed's background and other projects, software and otherwise.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License. You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS
# IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language
# governing permissions and limitations under the License.

"""
Miscellaneous useful stuff.

L{callAfterDeferred} is a cool little function that looks for a
C{Deferred} as an attribute of some namespace (i.e., object) and does
a call after it fires.

L{DeferredTracker} lets you to track and wait for deferreds without
actually having received a reference to them. L{DeferredLock} lets you
shut things down when you get the lock.

L{CallRunner} is used by L{threads.ThreadWorker} and
L{process.ProcessWorker}. You probably won't need to use it yourself,
unless perhaps you come up with an entirely new kind of
L{interfaces.IWorker} implementation.
"""

from time import time
import os, signal, pickle
import cProfile as profile
from contextlib import contextmanager

from twisted.internet import defer, reactor, protocol
from twisted.python.failure import Failure

from asynqueue import errors, info, iteration


def o2p(obj):
    """
    Converts an object into a pickle string or a blank string if an
    empty container.
    """
    if isinstance(obj, (list, tuple, dict)) and not obj:
        return b""
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

def killProcess(pid):
    """
    Kills the process with the supplied PID, returning a deferred that
    fires when it's no longer running.

    The return value is C{True} if the process was alive and had to be
    killed, C{False} if it was already dead.
    """
    def isDead():
        try:
            status = os.waitpid(pid, os.WNOHANG)[1]
        except:
            return True
        return status != 0

    def triedTerm(OK):
        if not OK:
            os.kill(pid, signal.SIGKILL)
            return iteration.Delay().untilEvent(
                isDead).addCallback(lambda _: True)
        return True

    if isDead():
        return defer.succeed(False)
    os.kill(pid, signal.SIGTERM)
    return iteration.Delay(timeout=5.0).untilEvent(
        isDead).addCallback(triedTerm)


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
    def add(self, x, y):
        return x+y
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

class ProcessProtocol(object):
    """
    I am a simple protocol for spawning a subordinate process.

    @ivar d: A C{Deferred} that fires with an initial chunk of STDOUT
        from the process.
    """
    def __init__(self, stopper=None):
        self.stopper = lambda x: None if stopper is None else stopper
        self.d = defer.Deferred()
        
    def makeConnection(self, pt):
        self.pid = pt.pid
        
    def childDataReceived(self, childFD, data):
        data = data.strip()
        if childFD == 1:
            if data and not self.d.called:
                self.d.callback(data)
        if childFD == 2:
            print("\nERROR: {}".format(data))
            #self.stopper(self.pid)
            
    def childConnectionLost(self, childFD):
        self.stopper(self.pid)
    def processExited(self, reason):
        self.stopper(self.pid)
    def processEnded(self, reason):
        self.stopper(self.pid)


class DeferredTracker(object):
    """
    I allow you to track and wait for Twisted C{Deferred} objects
    without actually having received a reference to them, or
    interfering with their callback chains.

    Uses instances of L{iteration.Delay} to check a count of pending
    deferreds that have been tracked with calls to L{put} or L{addWait}.
    """
    def __init__(self, interval=None, backoff=None):
        self.dCount = 0
        self.interval = interval
        self.backoff = backoff

    def addWait(self):
        """
        Adds a wait condition for me to track that gets removed when you
        call L{removeWait}.

        Calling this multiple times before release will add nested
        wait conditions. Make sure you do a call to L{removeWait} for
        each L{addWait} call!
        """
        if self.dCount is not None:
            self.dCount += 1

    def removeWait(self):
        """
        Removes a wait condition added by L{addWait}. Don't call this unless
        you've called L{addWait}!

        B{Caution:} Calling this will cause any pending L{deferToAny}
        calls to fire. It will also reduce the number of pending
        deferreds that any calls to L{deferUntilFewer} are waiting on.
        """
        if self.dCount: self.dCount -= 1

    def quitWaiting(self):
        """
        Call this to have me quit waiting for any pending or future
        Deferreds. Only use this if you B{really} don't need me to
        wait on anything ever again.

        Useful for quickly clearing everybody off the stage (and
        keeping them there!) during shutdown.
        """
        self.dCount = None
        
    def put(self, d):
        """
        Put another C{Deferred} in the tracker.
        """
        def transparentCallback(anything):
            self.removeWait()
            return anything

        self.addWait()
        d.addBoth(transparentCallback)
        return d

    def notWaiting(self):
        """
        Returns C{True} if I'm not waiting for any Deferreds to fire.
        """
        return not self.dCount
    
    def deferToAll(self, timeout=None):
        """
        Returns a C{Deferred} that tracks all active deferreds that haven't
        yet fired.

        When all the tracked deferreds fire, the returned deferred
        fires, too, with C{True}. The tracked deferreds do not get
        bogged down by the callback chain for the Deferred returned by
        this method.

        If the tracked deferreds never fire and a specified I{timeout}
        expires, the returned deferred will fire with
        C{False}. Calling L{quitWaiting} will make it fire almost
        immediately.
        """
        return iteration.Delay(
            interval=self.interval,
            backoff=self.backoff, timeout=timeout).untilEvent(self.notWaiting)

    def deferToAny(self, timeout=None):
        """
        Returns a C{Deferred} that fires with C{True} when B{any} of the
        active deferreds fire.

        The tracked deferreds do not get bogged down by the callback
        chain for the returned one. If some of them never fire and a
        I{timeout} specified in seconds expires, the returned deferred
        will fire with C{False}. Calling L{quitWaiting} will make it
        fire almost immediately, with C{True}.

        B{Caution:} Don't add any deferreds with calls to L{put} while
        waiting for this method to finish, because it will mess up the
        count. You will have to wait for one more deferred to fire for
        each new one you add before the returned deferred fires.
        """
        def oneFired(dCount):
            return self.dCount is None or self.dCount < dCount
        
        dCount = self.dCount
        # Local dCount has been frozen at my current dCount value
        return iteration.Delay(
            interval=self.interval,
            backoff=self.backoff, timeout=timeout).untilEvent(oneFired, dCount)

    def deferUntilFewer(self, N, timeout=None):
        """
        Returns a C{Deferred} that fires with C{True} when there are fewer
        than I{N} tracked deferreds pending.

        The tracked deferreds do not get bogged down by the callback
        chain for the returned one. If not enough of the tracked
        deferreds ever fire and a I{timeout} specified in seconds
        expires, the returned deferred will fire with
        C{False}. Calling L{quitWaiting} will make it fire almost
        immediately, with C{True}.

        You can add deferreds with calls to L{put} while waiting for
        this method to finish. That will just add to the number of
        tracked deferreds pending.

        See the U{source<http://edsuom.com/ade/ade.population.py>} for
        the U{ade<http://edsuom.com/ade.html>} package's C{Population}
        object to see a simple example of this in action. Here's the
        pertinent code::

            @defer.inlineCallbacks
            def populate():
                k = 0
                while running():
                    i = getIndividual()
                    if blank:
                        i.SSE = np.inf
                        addIndividual(i)
                        continue
                    k += 1
                    d = i.evaluate()
                    d.addCallback(evaluated, d)
                    d.addErrback(oops)
                    dt.put(d)
                    yield dt.deferUntilFewer(self.N_maxParallel)
                    if k >= self.Np:
                        break
                yield dt.deferToAll()

        The C{Deferred} that gets yielded while the loop is running
        fires immediately if there are fewer evaluations going on at
        once than the limit. Otherwise, it fires when enough
        evaluations finish to put things below the limit.
        """
        def fewEnoughPending():
            return self.dCount is None or self.dCount < N
        
        return iteration.Delay(
            interval=self.interval,
            backoff=self.backoff, timeout=timeout).untilEvent(fewEnoughPending)

    
class DeferredLock(defer.DeferredLock):
    """
    I am a modified form of C{defer.DeferredLock} lock that lets you
    shut things down when you get the lock.

    The I{allowZombies} keyword is ignored as the lock's behavior has
    been changed to always return an immediate C{Deferred} from a call
    to L{acquire} if the lock has been stopped. I mean, why the hell
    not?
    """
    def __init__(self, allowZombies=False):
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
        Like C{defer.DeferredLock.acquire} except with a I{vip}
        option.

        This lets you cut ahead of everyone in the regular waiting
        list and gets the next lock, after anyone else in the VIP line
        who is waiting from their own call of this method.

        If I'm stopped, calling this method simply returns an
        immediate C{Deferred}.
        """
        def transparentCallback(result):
            self.N_vips -= 1
            return result
        
        if not self.running:
            return defer.succeed(self)
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
        """
        Acquires the lock and immediately releases it.
        """
        return self.acquire(vip).addCallback(lambda x: x.release())

    def release(self):
        """
        Acts like Twisted's regular C{defer.DeferredLock.release} unless
        I'm stopped. Then calling this does nothing because the lock
        is acquired instantly in that condition.
        """
        if not self.running:
            return
        return super(DeferredLock, self).release()
        
    def addStopper(self, f, *args, **kw):
        """
        Add a callable (along with any args and kw) to be run when
        shutting things down.

        The callable may return a deferred, and more than one can be
        added. They will be called, and their result awaited, in the
        order received.
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

    @keyword raw: Set C{True} to return raw iterators by default instead
      of doing L{iteration} magic.
    
    @keyword callStats: Set C{True} to accumulate a list of
      I{callTimes} for each call. B{Caution:} Can get big with
      lots of calls!
    
    @keyword reactor: Set to an instance of
      C{twisted.internet.reactor} to have calls run in the
      reactor.
    """
    def __init__(self, raw=False, callStats=False, reactor=None):
        """C{CallRunner(raw=False, callStats=False, reactor=None)}"""
        self.raw = raw
        self.info = info.Info()
        self.callStats = callStats
        if callStats:
            self.callTimes = []
        self.reactor = reactor
        if reactor and not raw:
            raise ValueError("Only raw mode supported with a process reactor!")

    def __call__(self, callTuple):
        """
        Does the f-args-kw call in I{callTuple} to get a 2-tuple
        containing the status of the call and its result:

          - B{e}: An exception was raised; the result is a
            pretty-printed traceback string.
          
          - B{r}: Ran fine, the result is the return value of the
            call.
          
          - B{i}: Ran fine, but the result is an iterable other than a
            standard Python one.

        Honors the I{raw} option to return iterators as-is if
        desired. The called function never sees that keyword.
        """
        f, args, kw = callTuple
        if self.reactor:
            args = [self.reactor, f] + list(args)
            f = threads.blockingCallFromThread
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
            return (b'e', result)
        if not raw and iteration.Deferator.isIterator(result):
            return (b'i', result)
        return (b'r', result)
