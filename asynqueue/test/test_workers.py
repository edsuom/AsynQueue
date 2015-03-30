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
Unit tests for asynqueue.workers
"""

import time, random, threading
import multiprocessing as mp
import zope.interface
from twisted.internet import defer, reactor

from testbase import deferToDelay, TestCase, ProcessProtocol, \
    errors, base, workers, tasks


class TestThreadWorker(TestCase):
    verbose = True
    
    def setUp(self):
        self.worker = workers.ThreadWorker()
        self.queue = base.TaskQueue()
        self.queue.attachWorker(self.worker)

    def tearDown(self):
        return self.queue.shutdown()

    def _blockingTask(self, x):
        delay = random.uniform(0.1, 0.5)
        self.msg(
            "Running {:f} sec. task in thread {}",
            delay, threading.currentThread().getName())
        time.sleep(delay)
        return 2*x

    def test_shutdown(self):
        def checkStopped(null):
            self.assertFalse(self.worker.t.threadRunning)

        d = self.queue.call(self._blockingTask, 0)
        d.addCallback(lambda _: self.queue.shutdown())
        d.addCallback(checkStopped)
        return d

    def test_shutdownWithoutRunning(self):
        def checkStopped(null):
            self.assertFalse(self.worker.t.threadRunning)

        d = self.queue.shutdown()
        d.addCallback(checkStopped)
        return d

    def test_stop(self):
        def checkStopped(null):
            self.assertFalse(self.worker.t.threadRunning)

        d = self.queue.call(self._blockingTask, 0)
        d.addCallback(lambda _: self.worker.stop())
        d.addCallback(checkStopped)
        return d

    def test_oneTask(self):
        d = self.queue.call(self._blockingTask, 15)
        d.addCallback(self.assertEqual, 30)
        return d

    def test_multipleTasks(self):
        N = 5
        expected = [2*x for x in xrange(N)]
        for k in self.multiplerator(N, expected):
            self.d = self.queue.call(self._blockingTask, k)
        return self.dm

    def test_multipleCalls(self):
        N = 5
        expected = [('r', 2*x) for x in xrange(N)]
        worker = workers.ThreadWorker()
        for k in self.multiplerator(N, expected):
            task = tasks.Task(self._blockingTask, (k,), {}, 0, None)
            self.d = task.d
            worker.run(task)
        return self.dm.addCallback(lambda _: worker.stop())
        
    def test_multipleWorkers(self):
        N = 20
        mutable = []

        def gotResult(result):
            self.msg("Task result: {}", result)
            mutable.append(result)

        def checkResults(null):
            self.assertEqual(len(mutable), N)
            self.assertEqual(
                sum(mutable),
                sum([2*x for x in xrange(N)]))

        # Create and attach two more workers, for a total of three
        for null in xrange(2):
            worker = workers.ThreadWorker()
            self.queue.attachWorker(worker)
        dList = []
        for x in xrange(N):
            d = self.queue.call(self._blockingTask, x)
            d.addCallback(gotResult)
            dList.append(d)
        d = defer.DeferredList(dList)
        d.addCallback(checkResults)
        return d


class TestAsyncWorker(TestCase):
    verbose = True
    
    def setUp(self):
        self.worker = workers.AsyncWorker()
        self.queue = base.TaskQueue()
        self.queue.attachWorker(self.worker)

    def tearDown(self):
        return self.queue.shutdown()

    def _twistyTask(self, x):
        delay = random.uniform(0.1, 0.5)
        self.msg("Running {:f} sec. async task", delay)
        return deferToDelay(delay).addCallback(lambda _: 2*x)
        
    def test_call(self):
        d = self.queue.call(self._twistyTask, 2)
        d.addCallback(self.assertEqual, 4)
        return d

    def test_multipleTasks(self):
        N = 5
        expected = [2*x for x in xrange(N)]
        for k in self.multiplerator(N, expected):
            self.d = self.queue.call(self._twistyTask, k)
        return self.dm

    def test_multipleCalls(self):
        N = 5
        expected = [('r', 2*x) for x in xrange(N)]
        worker = workers.AsyncWorker()
        for k in self.multiplerator(N, expected):
            task = tasks.Task(self._twistyTask, (k,), {}, 0, None)
            self.d = task.d
            worker.run(task)
        # NOTE: Hangs here
        return self.dm.addCallback(lambda _: worker.stop())


class TestProcessWorker(TestCase):
    verbose = True
    
    def setUp(self):
        from pserver import TestStuff
        self.stuff = TestStuff()
        self.worker = workers.ProcessWorker()
        self.queue = base.TaskQueue()
        self.queue.attachWorker(self.worker)
        self.pid = str(self.worker.pt.pid)

    def tearDown(self):
        return self.queue.shutdown()

    @defer.inlineCallbacks
    def checkStopped(self, *args):
        yield deferToDelay(0.5)
        print "PID", self.pid
        pp = ProcessProtocol(self.verbose)
        args = ("/bin/ps", '-p', self.pid, '--no-headers')
        pt = reactor.spawnProcess(pp, args[0], args)
        stdout = yield pp.waitUntilReady()
        self.assertEqual(stdout, "")
        yield pt.loseConnection()

    @defer.inlineCallbacks
    def test_basic(self):
        result = yield self.queue.call(
            "asynqueue.pserver.divide", 5.0, 2.0)
        self.assertEqual(result, 2.5)
        yield self.queue.shutdown()
        yield self.checkStopped()
    
    def test_shutdownWithoutRunning(self):
        d = self.queue.shutdown()
        d.addCallback(self.checkStopped)
        return d

    def test_stop(self):
        d = self.queue.call(self._blockingTask, 0, thread=True)
        d.addCallback(lambda _: self.worker.stop())
        d.addCallback(self.checkStopped)
        return d

    def test_oneTask(self):
        d = self.queue.call(self._blockingTask, 15, thread=True)
        d.addCallback(self.assertEqual, 30)
        return d

    def test_multipleWorkers(self):
        N = 20
        mutable = []

        def gotResult(result):
            if VERBOSE:
                print "Task result: %s" % result
            mutable.append(result)

        def checkResults(null):
            self.assertEqual(len(mutable), N)
            self.assertEqual(
                sum(mutable),
                sum([2*x for x in xrange(N)]))

        # Create and attach two more workers, for a total of three
        for null in xrange(2):
            worker = workers.ProcessWorker()
            self.queue.attachWorker(worker)
        dList = []
        for x in xrange(N):
            d = self.queue.call(self._blockingTask, x, thread=True)
            d.addCallback(gotResult)
            dList.append(d)
        d = defer.DeferredList(dList)
        d.addCallback(checkResults)
        return d
