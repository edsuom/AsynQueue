# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
#
# Copyright (C) 2006-2007, 2015 by Edwin A. Suominen,
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
L{ThreadQueue}, L{ThreadWorker} and their support staff. Also, a
cool implementation of the oft-desired C{deferToThread}, in
L{ThreadQueue.deferToThread}.
"""

import threading

from zope.interface import implements
from twisted.internet import defer, reactor
from twisted.python.failure import Failure
from twisted.internet.interfaces import IConsumer, IPushProducer


from base import TaskQueue
from interfaces import IWorker
import errors, util, iteration


_DTL = [None]
def deferToThread(*fargs, **kw):
    """
    Module-level function that lets you call a function in a dedicated
    thread and get a C{Deferred} to its result, with no fuss on your
    part. The thread will remain alive and will be used for further
    calls to this function and this function only.

    Call with I{f}, I{*args}, and I{**kw} as usual.

    This is AsynQueue's single-threaded, queued, I{doNext}-able,
    L{iteration.Deferator}-able answer to Twisted's C{deferToThread}.

    If you expect a deferred iterator as your result (an instance of
    L{iteration.Deferator}), supply an C{IConsumer} implementor via
    the I{consumer} keyword. Each iteration will be written to it, and
    the deferred will fire when the iterations are done. Otherwise,
    the deferred will fire with an L{iteration.Deferator}.
    
    If you want to kill the dedicated thread, just call this function
    with no arguments, not even a callable object I{f}. A C{Deferred}
    will be returned that fires when the thread is gone.
    """
    if not fargs:
        tl = _DTL[0]
        if tl is None:
            return defer.succeed(None)
        return tl.stop()
    if _DTL[0] is None:
        tl = _DTL[0] = ThreadLooper()
        reactor.addSystemEventTrigger('before', 'shutdown', tl.stop)
    return _DTL[0].deferToThread(*fargs, **kw)
    

class ThreadQueue(TaskQueue):
    """
    I am a L{TaskQueue} for dispatching arbitrary callables to be run
    by a single worker thread.
    """
    def __init__(self, **kw):
        raw = kw.pop('raw', False)
        TaskQueue.__init__(self, **kw)
        self.worker = ThreadWorker(raw=raw)
        self.d = self.attachWorker(self.worker)

    def deferToThread(self, f, *args, **kw):
        """
        Runs the f-args-kw call in my dedicated worker thread, skipping
        past the queue. As with a regular L{TaskQueue.call}, returns a
        C{Deferred} that fires with the result and deals with
        iterators.
        """
        return util.callAfterDeferred(
            self, 'd', self.worker.t.deferToThread, f, *args, **kw)


class ThreadWorker(object):
    """
    I implement an L{IWorker} that runs tasks in a dedicated worker
    thread.

    @cvar cQualified: Task series all instances of me are qualified to
      perform.

    @ivar iQualified: Task series one instance of me is qualified to
      perform. Usually left blank, unless you want only some workers
      doing certain tasks.
    """
    implements(IWorker)
    cQualified = ['thread', 'local']

    def __init__(self, series=[], raw=False):
        """
        Constructs me with a L{ThreadLooper} and an empty list of tasks.
        
        @param series: A list of one or more task series that this
          particular instance of me is qualified to handle.

        @param raw: Set C{True} if you want raw iterators to be
          returned instead of L{iteration.Deferator} instances. You
          can override this in with the same keyword set C{False} in a
          call.
        """
        self.tasks = []
        self.iQualified = series
        self.t = ThreadLooper(raw)

    def setResignator(self, callableObject):
        self.t.dLock.addStopper(callableObject)

    def run(self, task):
        """
        Returns a C{Deferred} that fires only after the threaded call is
        done.

        I do basic FIFO queuing of calls to this method, but priority
        queuing is above my paygrade and you'd best honor my deferred
        and let someone like L{tasks.TaskHandler} only call this
        method when I say I'm ready.

        One simple thing I B{will} do is apply the I{doNext} keyword
        to any task with the highest priority, -20 or lower (for a
        L{base.TaskQueue.call} with its own I{doNext} keyword set). If
        you call this method one task at a time like you're supposed
        to, even that won't make a difference, except that it will cut
        in front of any existing call with I{doNext} set. So use
        judiciously.
        """
        def done(statusResult):
            if task in self.tasks:
                self.tasks.remove(task)
            if statusResult[0] == 'i':
                # What we got is a Deferator, but if a consumer was
                # supplied, we need to couple an IterationProducer to
                # it and fire the task callback with the deferred from
                # running the producer.
                if consumer:
                    dr = statusResult[1]
                    ip = iteration.IterationProducer(dr, consumer)
                    statusResult = ('i', ip)
            task.d.callback(statusResult)
        
        self.tasks.append(task)
        f, args, kw = task.callTuple
        consumer = kw.pop('consumer', None)
        if task.priority <= -20:
            kw['doNext'] = True
        return self.t.call(f, *args, **kw).addCallback(done)

    def stop(self):
        """
        @return: A C{Deferred} that fires when the task loop has ended and
          its thread has terminated.
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


class ThreadLooper(object):
    """
    I run function calls in a dedicated thread.

    Each call returns a C{Deferred} to its eventual result, which is a
    2-tuple containing the status of the last call and its result
    according to the format of L{util.CallRunner}.

    If the result is an iterable other than one of Python's built-in
    ones, the C{Deferred} fires with an instance of
    L{iteration.Prefetcherator} instead. Couple it to your own
    deferator to iterate over the underlying iterable running in my
    thread. You can disable this behavior by setting C{raw=True} in
    the constructor, or enable/disable it on an individual call by
    setting raw=True/False.

    @ivar timeout: The wait timeout, which defaults to 60 (one
      minute). This is just how long the thread loop waits before
      checking for a pending deferred and firing it with a timeout
      error. Otherwise, it simply waits another minute, and it can do
      that forever with no problem.
    """
    timeout = 60
    
    def __init__(self, raw=False):
        # Just a simple attribute to indicate if the thread loop is
        # running, mostly for unit testing
        self.threadRunning = True
        # Tools
        self.runner = util.CallRunner(raw)
        self.dLock = util.DeferredLock()
        self.event = threading.Event()
        self.thread = threading.Thread(name=repr(self), target=self.loop)
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

    @defer.inlineCallbacks
    def call(self, f, *args, **kw):
        """
        Runs the supplied callable function with any args and keywords in
        a dedicated thread, returning a deferred that fires with a
        status/result tuple.

        Calls are done in the order received, unless you set
        C{doNext=True}.

        Set C{raw=True} to have a raw iterator returned instead of a
        Deferator, or C{raw=False} to have a L{Deferator} returned
        instead of a raw iterator, contrary to the instance-wide
        default set with the constructor keyword 'raw'.
        """
        yield self.dLock.acquire(kw.pop('doNext', False))
        self.callTuple = f, args, kw
        self.d = defer.Deferred()
        # The callTuple is set for this call along with the deferred
        # to be called back with its result, so release the thread to
        # work on it, firing this deferred's callback with its result.
        self.event.set()
        statusResult = yield self.d
        # The deferred lock is released after the call is done so
        # that another call can proceed. This is NOT the same as
        # the event used as a threading lock. It keeps the main
        # thread from setting that event before the thread loop is
        # ready for that.
        self.dLock.release()
        status, result = statusResult
        if status == 'i':
            ID = str(hash(result))
            pf = iteration.Prefetcherator(ID)
            ok = yield pf.setup(result)
            if ok:
                # OK, we can iterate this
                result = iteration.Deferator(
                    repr(pf), self.deferToThread, pf.getNext)
            else:
                # An iterator, but not one we could prefetch
                # from. Probably empty.
                result = []
        # Not an iterator, at least not one being specially
        # processed; we already have our result
        defer.returnValue((status, result))

    def dr2ip(self, dr, consumer=None):
        """
        Converts a L{Deferator} into an L{iteration.IterationProducer},
        with a consumer registered if you supply one. Then each
        iteration will be written to your consumer, and the deferred
        returned will fire when the iterations are done. Otherwise,
        the deferred will fire with an L{iteration.IterationProducer}
        and you will have to register with and run it yourself.
        """
        ip = iteration.IterationProducer(dr)
        if consumer:
            ip.registerConsumer(consumer)
            return ip.run()
        return ip
        
    def deferToThread(self, f, *args, **kw):
        """
        My single-threaded, queued, doNext-able, Deferator-able answer to
        Twisted's deferToThread.

        If you expect a deferred iterator as your result (an instance
        of L{iteration.Deferator}), supply an C{IConsumer} implementor
        via the I{consumer} keyword. Each iteration will be written to
        it, and the deferred will fire when the iterations are
        done. Otherwise, the deferred will fire with an
        L{iteration.Deferator}.
        """
        def done(statusResult):
            status, result = statusResult
            if status == 'e':
                return Failure(errors.ThreadError(result))
            elif status == 'i':
                if consumer:
                    ip = iteration.IterationProducer(dr, consumer)
                    return ip.run()
                return result
            return result

        consumer = kw.pop('consumer', None)
        return self.call(f, *args, **kw).addCallback(done)

    def stop(self):
        """
        @return: A C{Deferred} that fires when the task loop has ended and
          its thread has terminated.
        """
        if not self.threadRunning:
            return defer.succeed(None)
        # Tell the thread to quit with a null task
        self.callTuple = None
        self.event.set()
        # Now stop the lock
        self.dLock.addStopper(self.thread.join)
        return self.dLock.stop()


class IterationGetter(object):
    """
    Abstract base class for objects that munch data on one end and act
    like iterators to yield it on the other end.

    @see: L{Consumerator} and L{Filerator}.
    """
    class IterationStopper:
        pass
    
    def __init__(self):
        self.runState = 'init'
        self.d = defer.Deferred()
        # Locks for my iteration-consuming thread, the
        # blocking-iterator thread, and the next-iteration event
        self.cLock = threading.Semaphore()
        self.bLock = threading.Lock()
        self.nLock = threading.Lock()
        # Lock both of my iteration-processing loops until an
        # iteration is received
        self.cLock.acquire()
        self.bLock.acquire()
        # We leave the next-iteration lock unlocked; the
        # iteration-consuming thread will lock it to overwrite the
        # blocking-iterator thread's value of each iteration

    def start(self):
        """
        Call this when I should start listening for iterations.
        """
        self.thread = threading.Thread(name=repr(self), target=self.loop)
        self.thread.start()

    def loop(self):
        """
        @see: L{Consumerator.loop} and L{Filerator.loop}
        """
        raise NotImplementedError("You must override this in a subclass")

    def deferUntilDone(self):
        """
        Returns a C{Deferred} that fires when I am done iterating.
        """
        d = defer.Deferred()
        self.d.chainDeferred(d)
        return d
        
    # Iterator implementation -------------------------------------------------
    # Call in its own thread

    def __iter__(self):
        return self

    def next(self):
        # Wait for the next iteration to be produced
        self.bLock.acquire()
        # Get a local reference to the iteration value
        value = self.bIterationValue
        # Now it can be changed, so release my iteration-consuming
        # loop to do so
        self.nLock.release()
        if isinstance(value, self.IterationStopper):
            # We are done iterating. The blocking caller will
            # immediately exit its loop.
            raise StopIteration
        # This is a legit iteration value, return it. Since this
        # method runs in the blocking-iterator thread, it won't
        # get called again until the caller is ready for another
        # iteration.
        return value


class Consumerator(IterationGetter):
    """
    I act like an C{IConsumer} for your Twisted code and an iterator
    for your blocking code running via a L{ThreadWorker}. This is
    handy when you are using a conventional library that relies on an
    iterator as its input::

      def render(request):
          w = png.Writer()
          c = asynqueue.Consumerator()
          c.deferUntilDone().addCallback(lambda _: request.finish())
          p = self.producePixelRows(c)
          w.write(request, c)
          return server.NOT_DONE_YET

    I work with either an I{IPushProducer} or an I{IPullProducer}. You
    can construct me with an instance of the former and I'll get
    started right away. Otherwise, call my L{registerProducer} method
    with the producer and whether it is streaming (push) or not.

    @ivar runState: 'init', 'running', 'stopping', 'stopped'

    @ivar d: A C{Deferred} that fires when iterations are done.
    """
    implements(IConsumer)

    def __init__(self, producer=None):
        """
        @param producer: The producer for me to register, if you want to
          supply an C{IPushProducer} one on instantiation. Otherwise,
          use L{registerProducer}.
        """
        super(Consumerator, self).__init__()
        self.dLock = util.DeferredLock()
        if producer:
            self.registerProducer(producer, True)

    def loop(self):
        """
        Runs a loop in a dedicated thread that waits for new iterations to
        be produced. When I get an instance of
        L{IterationGetter.IterationStopper}, the loop exits. I then
        call my "all done" C{Deferred} and delete my reference to the
        producer.
        """
        self.runState = 'running'
        while True:
            # Wait for an iteration
            self.cLock.acquire()
            # Get a copy of the value
            value = self.cIterationValue
            # Release the consumer interface to write another
            # iteration
            reactor.callFromThread(self.dLock.release)
            # Wait until it's safe to overwrite the blocking-iterator
            # loop's copy
            self.nLock.acquire()
            # Now do so and release it to work on the new copy
            self.bIterationValue = value
            self.bLock.release()
            if isinstance(value, self.IterationStopper):
                # This was the post-iteration signal; this loop is now
                # done.
                break
        # Wait until we know the iteration stopper was noticed and the
        # blocking iterations stopped.
        self.runState = 'stopping'
        self.nLock.acquire()
        reactor.callFromThread(self.d.callback, None)
        self.runState = 'stopped'
        reactor.callFromThread(delattr, self, 'producer')
        
    def stop(self):
        """
        Good manners urge you to call this to cleanly break out of a loop
        of my iterations so that my producer doesn't keep working for
        nothing. Calling this method at the Twisted main-loop level is
        also a fine way to quit producing and iterating when you know
        you're done.

        Not part of the official iterator implementation, but
        useful for a Twisted way of iterating. You need a way of
        letting whatever is producing the iterations know that there
        won't be any more of them.
        """
        if hasattr(self, 'producer'):
            self.producer.stopProducing()
        return self.unregisterProducer()
        
    # --- IConsumer implementation --------------------------------------------

    def write(self, data):
        """
        The producer calls this with a chunk of I{data}. It goes through
        two stages to emerge from my blocking end as an iteration, via
        L{next}.
        """
        def handleData(null, x):
            self.cIterationValue = x
            if self.runState == 'running':
                # Release my iteration-consuming loop to work on the next
                # iteration value
                self.cLock.release()
                # The producer can and should write another iteration now
                self.producer.resumeProducing()

        if self.streaming and self.runState == 'running':
            # The producer is a IPushProducer, so tell it to hold off
            # on any more iteration values for the moment while
            # everything it's sent (and may yet send) gets processed
            self.producer.pauseProducing()
        # Handle the data in the order received
        return self.dLock.acquire().addCallback(handleData, data)
    
    def registerProducer(self, producer, streaming):
        """
        C{IConsumer} implementation
        """
        if hasattr(self, 'producer'):
            raise RuntimeError()
        self.producer = producer
        self.streaming = streaming
        if not streaming:
            producer.resumeProducing()
        self.start()

    def unregisterProducer(self):
        """
        C{IConsumer} implementation
        """
        if not hasattr(self, 'producer'):
            return defer.succeed(None)
        return self.write(self.IterationStopper())
        

class Filerator(IterationGetter):
    """
    Stream data to me in one end and I will iterate it out the other.
    
    Acts like a file handle for writing in one thread (even the main
    one under the Twisted event loop) and an iterator in another
    thread. Hook me up to an L{iteration.Deferator} to stream data
    over a worker interface.

    You must call my L{close} method to stop me from iterating.
    """
    def __init__(self):
        super(Filerator, self).__init__()
        self.itemBuffer = []
        self.start()

    @property
    def closed(self):
        return self.runState == 'stopped'
        
    def loop(self):
        """
        Runs a loop in a dedicated thread that waits for new iterations to
        be written. When I get an instance of
        L{IterationGetter.IterationStopper}, the loop exits.
        """
        self.runState = 'running'
        while True:
            # Wait for an iteration
            self.cLock.acquire()
            # Get the oldest value in the FIFO buffer
            value = self.itemBuffer.pop(0)
            # Wait until it's safe to overwrite the blocking-iterator
            # loop's copy
            self.nLock.acquire()
            # Now do so and release it to work on the new copy
            self.bIterationValue = value
            self.bLock.release()
            if isinstance(value, self.IterationStopper):
                # This was the post-iteration signal; this loop is now
                # done.
                break
        # Wait until we know the iteration stopper was noticed and the
        # blocking iterations stopped.
        self.runState = 'stopping'
        self.nLock.acquire()
        self.runState = 'stopped'
        reactor.callFromThread(self.d.callback, None)
    
    def write(self, data):
        """
        This is called with a chunk of I{data}. It goes through two stages
        to emerge from my blocking end as an iteration, via L{next}.
        """
        if self.closed:
            raise ValueError("Closed, not accepting writes")
        self.itemBuffer.append(data)
        if self.runState == 'running':
            # Release my iteration-consuming loop to work on the next
            # iteration value. The cLock object is actually a
            # semaphore, so it's OK if this gets called multiple times
            # before the other loop can acquire it again.
            self.cLock.release()
        
    def writelines(self, lines):
        """
        Adds a list full of data chunks to my buffer.
        """
        for line in lines:
            self.write(line)
    
    def flush(self):
        """
        Doesn't do anything, because I am always trying to flush my buffer
        by iterating its contents.
        """
        
    def close(self):
        """
        Closing me as a "file" tells me that I can stop iterating once the
        buffer is flushed.
        """
        if not self.closed:
            self.write(self.IterationStopper())


class OrderedItemProducer(object):
    """
    Produces blocking iterations in the order they are requested via
    an asynchronous function call.
    
    I am an implementor of Twisted's C{IPushProducer} interface that
    produces an iteration to a blocking call I{fb} for every time you
    call a non-blocking item-generating function I{fb} via my
    L{produceItem} method. Significantly, the items are buffered as
    needed so that the iterations appear in the order of the calls to
    L{produceItem} that generated them, B{not} necessarily in the
    order in which they are actually generated.

    Start things off by constructing an instance of me, with an
    existing task queue if you have one you want me to use, and then
    running L{start} with your blocking f-args-kw combination. Then
    call L{produceItem} repeatedly with whatever f-args-kw combination
    results (eventually) in new items to iterate. These calls may
    return deferred results and should not block.

    When you are done having me produce iterations, call
    L{stop}. Whatever loop the blocking-iterator call is in will then
    terminate and function I{fb} should end.
    
    @ivar i: My L{Consumerator} instance, which acts like an iterator
      for whatever function you supply to L{start}.

    @ivar q: The L{TaskQueue} instance I use, either supplied by you
      during construction or instantiated by me. Either way, you will
      have to call L{TaskQueue.shutdown} on this eventually when
      you're done with the queue.
    """
    implements(IPushProducer)
    
    def __init__(self):
        self.itemBuffer = {}
        self.k1, self.k2 = 0, 0
        self.seriesID = hash(self)
        self.i = Consumerator(self)
        self.dLock = defer.DeferredLock()
        self.dt = util.DeferredTracker()
        self.dLock.acquire()
        self.produce = True

    def start(self, fb, *args, **kw):
        """
        Starts the blocking function call C{fb(i, *args, **kw)} that
        relies on my L{Consumerator} instance I{i} for iterations, in
        traditional blocking fashion. The function must accept C{i} as
        its first argument, and can also accept further arguments
        C{*args} and keywords C{**kw}, which you can specify in your
        call to L{start}.

        @return: A C{Deferred} that fires when the blocking call has
          started in a dedicated thread. Shouldn't take long at all.
        """
        def started():
            self.dLock.release()
            dStarted.callback(None)
        def runner():
            # This function runs via the queue in my dedicated thread
            reactor.callFromThread(started)
            # The actual blocking call
            result = fb(self.i, *args, **kw)
            reactor.callFromThread(finished, result)
        def finished(result):
            self.dFinished.callback(result)
            self.stopProducing()
        dStarted = defer.Deferred()
        self.dFinished = defer.Deferred()
        thread = threading.Thread(name=repr(self), target=runner)
        thread.start()
        return dStarted
    
    def produceItem(self, fp, *args, **kw):
        """
        Runs C{fp(*args, **kw)} to generate an item that I produce as an
        iteration to whatever blocking call was (or will be) set
        running via L{start}.

        While I am running, the returned C{Deferred} fires after the
        call to I{fp} with the item value produced by the call to
        I{f}. You don't need to do anything with these deferreds if
        you don't want to use them for concurrency limiting; they are
        accounted for in L{stop}.

        If my L{stopProducing} method has been called, I no longer
        produce iterations and calls to this method do not run
        I{fp}. The returned C{Deferred} fires immediately with C{None}.
        """
        def gotItem(item):
            # We have a result, but we need to wait our turn to
            # actually produce it
            return self.dLock.acquire().addCallback(gotLock, item)
        def gotLock(lock, item):
            if self.k2 == k1 and self.produce:
                self._writeItem(item)
            elif self.produce is not None:
                self.itemBuffer[k1] = item
                self._flushBuffer()
            self.dLock.release()
            return item
        if self.produce is None:
            return defer.succeed(None)
        k1 = self.k1
        self.k1 += 1
        d = defer.maybeDeferred(fp, *args, **kw).addCallback(gotItem)
        self.dt.put(d)
        return d

    @defer.inlineCallbacks
    def stop(self):
        """
        Call this to indicate that iterations are done. After any pending
        calls from L{produceItem} are done, my L{Consumerator} will
        raise L{StopIteration} for the blocking iteration-caller in
        I{fb} and that function should exit. Whatever value it returns
        will fire the C{Deferred} that is returned here.

        If I{fb} exited early for some reason, the C{Deferred} this
        method returns will have fired already.

        Repeated calls to this method make no sense and will be
        rewarded with deferreds immediately firing with C{None}.
        """
        yield self.dt.deferToAll()
        yield self.dLock.acquire()
        yield self.i.stop()
        if hasattr(self, 'dFinished'):
            result = yield self.dFinished
            del self.dFinished
        else:
            result = None
        self.dLock.release()
        defer.returnValue(result)
        
    def _writeItem(self, item):
        self.i.write(item)
        self.k2 += 1
        self._flushBuffer()

    def _flushBuffer(self):
        if self.k2 in self.itemBuffer:
            item = self.itemBuffer.pop(self.k2)
            # This will result in another call to resumeProducing
            self._writeItem(item)
        
    def stopProducing(self):
        self.produce = None

    def pauseProducing(self):
        self.produce = False

    def resumeProducing(self):
        self.produce = True
