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
ThreadWorker and its support staff. Also, a cool implementation of
deferToThread.
"""

import threading

from zope.interface import implements
from twisted.internet import defer, reactor
from twisted.python.failure import Failure

from base import TaskQueue
from interfaces import IWorker
import errors, util, iteration


class ThreadQueue(TaskQueue):
    """
    I am a task queue for dispatching arbitrary callables to be run by
    a single worker thread.
    """
    def __init__(self, **kw):
        raw = kw.pop('raw', False)
        TaskQueue.__init__(self, **kw)
        self.worker = ThreadWorker(raw=raw)
        self.d = self.attachWorker(self.worker)

    def deferToThread(self, f, *args, **kw):
        """
        Runs the f-args-kw call in my dedicated worker thread, skipping
        past the queue. As with a regular TaskQueue.call, returns a
        deferred that fires with the result and deals with iterators.
        """
        return util.callAfterDeferred(
            self, 'd', self.worker.t.deferToThread, f, *args, **kw)


class ThreadWorker(object):
    """
    I implement an L{IWorker} that runs tasks in a dedicated worker
    thread.

    You can supply a series keyword containing a list of one or more
    task series that I am qualified to handle, and raw=True if you
    want raw iterators to be returned instead of prefetcherators
    unless otherwise specified in a call.
    """
    implements(IWorker)
    cQualified = ['thread', 'local']

    def __init__(self, series=[], profiler=None, raw=False):
        self.tasks = []
        self.iQualified = series
        self.profiler = profiler
        self.t = ThreadLooper(raw=raw)

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
        # TODO: Have thread run with self.profiler.runcall if profiler present
        return self.t.call(f, *args, **kw).addCallback(done)

    def stop(self):
        """
        The returned deferred fires when the task loop has ended and its
        thread terminated.
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
    I run function calls in a dedicated thread, returning a deferred
    to each eventual result, a 2-tuple containing the status of the
    last call and its result according to the format of
    L{util.CallRunner}.

    If the result is an iterable other than one of Python's built-in
    ones, the deferred fires with an instance of
    L{iteration.Prefetcherator} instead. Couple it to your own
    deferator to iterate over the underlying iterable running in my
    thread. You can disable this behavior by setting raw=True in the
    constructor, or enable/disable it on an individual call by setting
    raw=True/False.
    """
    # My default wait timeout is one minute: This is just how long the
    # thread loop waits before checking for a pending deferred and
    # firing it with a timeout error. Otherwise, it simply waits
    # another minute, and it can do that forever with no problem.
    timeout = 60
    
    def __init__(self, raw=False):
        self.raw = raw
        # Just a simple attribute to indicate if the thread loop is
        # running, mostly for unit testing
        self.threadRunning = True
        # Tools
        self.info = util.Info()
        self.runner = util.CallRunner()
        self.dLock = util.DeferredLock()
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

        Calls are done in the order received, unless you set
        doNext=True.

        Set raw=True to have a raw iterator returned instead of a
        Deferator, or raw=False to have a Deferator returned
        instead of a raw iterator, contrary to the instance-wide
        default set with the constructor keyword 'raw'.
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
            status, result = statusResult
            if status == 'i':
                # An iterator
                if raw:
                    # ...but no special processing
                    status = 'r'
                else:
                    # ...with special processing
                    ID = str(hash(result))
                    pf = iteration.Prefetcherator(ID)
                    if pf.setup(result):
                        # OK, we can iterate this
                        return ('i', iteration.Deferator(
                            repr(pf), self.deferToThread, pf.getNext))
                    # An iterator, but not a proper one
                    return ('e', "Failed to iterate for call {}".format(
                        self.info.setCall(f, args, kw).aboutCall()))
            # Not an iterator, at least not one being specially
            # processed; we already have our result
            return status, result

        raw = kw.pop('raw', None)
        if raw is None:
            raw = self.raw
        return self.dLock.acquire(
            kw.pop('doNext', False)).addCallback(threadReady)

    def dr2ip(self, dr, consumer=None):
        """
        Converts a Deferator into an iterationProducer, with a
        consumer registered if you supply one. Then each iteration
        will be written to your consumer, and the deferred returned
        will fire when the iterations are done. Otherwise, the
        deferred will fire with an L{iteration.IterationProducer} and
        you will have to register with and run it yourself.
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
        of L{iteration.Deferator}), supply an IConsumer implementor
        via the consumer keyword. Each iteration will be written to
        it, and the deferred will fire when the iterations are
        done. Otherwise, the deferred will fire with an
        L{iteration.Deferator}.
        """
        def done(statusResult):
            status, result = statusResult
            if status == 'e':
                return Failure(errors.WorkerError(result))
            if status == 'i' and not raw:
                if consumer:
                    ip = iteration.IterationProducer(dr, consumer)
                    return ip.run()
                return result
            return result

        raw = kw.pop('raw', None)
        if raw is None:
            raw = self.raw
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

        
