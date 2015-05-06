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
Unit tests for asynqueue.threads
"""

import time, random, threading
from twisted.internet import defer, reactor

from util import TestStuff
import base, tasks, iteration, threads
from testbase import deferToDelay, IterationConsumer, TestCase


class TaskMixin:
    def _blockingTask(self, x):
        delay = random.uniform(0.0, 0.2)
        self.msg(
            "Running {:f} sec. task in thread {}",
            delay, threading.currentThread().getName())
        time.sleep(delay)
        return 2*x

    def _producterator(self, x, N=7):
        for y in xrange(N):
            yield x*y


class TestThreadQueue(TaskMixin, TestCase):
    verbose = False

    def tearDown(self):
        if hasattr(self, 'q'):
            return self.q.shutdown()

    @defer.inlineCallbacks
    def test_basic(self):
        for raw in (True, False):
            q = threads.ThreadQueue(raw=raw)
            y = yield q.call(self._blockingTask, 1.5)
            self.assertEqual(y, 3.0)
            yield q.shutdown()

    @defer.inlineCallbacks
    def test_iteration_raw(self):
        q = threads.ThreadQueue(raw=True)
        iterator = yield q.call(self._producterator, 2.0)
        for k, y in enumerate(iterator):
            self.assertEqual(y, 2.0*k)
        yield q.shutdown()

    @defer.inlineCallbacks
    def test_iteration(self):
        q = threads.ThreadQueue()
        dr = yield q.call(self._producterator, 2.0)
        self.assertIsInstance(dr, iteration.Deferator)
        for k, d in enumerate(dr):
            y = yield d
            self.assertEqual(y, 2.0*k)
        yield q.shutdown()

        
class TestThreadWorker(TaskMixin, TestCase):
    verbose = False
    
    def setUp(self):
        self.worker = threads.ThreadWorker()
        self.queue = base.TaskQueue()
        self.queue.attachWorker(self.worker)

    def tearDown(self):
        return self.queue.shutdown()

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
        worker = threads.ThreadWorker()
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
            worker = threads.ThreadWorker()
            self.queue.attachWorker(worker)
        dList = []
        for x in xrange(N):
            d = self.queue.call(self._blockingTask, x)
            d.addCallback(gotResult)
            dList.append(d)
        d = defer.DeferredList(dList)
        d.addCallback(checkResults)
        return d

    @defer.inlineCallbacks
    def test_iteration(self):
        N1, N2 = 20, 50
        stuff = TestStuff()
        stuff.setStuff(N1, N2)
        dr = yield self.queue.call(stuff.stufferator)
        self.assertIsInstance(dr, iteration.Deferator)
        chunks = []
        for k, d in enumerate(dr):
            chunk = yield d
            self.msg("Chunk #{:d}: '{}'", k+1, chunk)
            self.assertEqual(len(chunk), N1)
            chunks.append(chunk)
        self.assertEqual(len(chunks), N2)

    @defer.inlineCallbacks
    def test_iterationProducer(self):
        N1, N2 = 20, 50
        stuff = TestStuff()
        stuff.setStuff(N1, N2)
        consumer = IterationConsumer(self.verbose)
        yield self.queue.call(stuff.stufferator, consumer=consumer)
        for chunk in consumer.data:
            self.assertEqual(len(chunk), N1)
        self.assertEqual(len(consumer.data), N2)

    @defer.inlineCallbacks
    def test_iteration_raw(self):
        N1, N2 = 5, 10
        stuff = TestStuff()
        stuff.setStuff(N1, N2)
        result = yield self.queue.call(stuff.stufferator, raw=True)
        count = 0
        for chunk in stuff.stufferator():
            self.assertEqual(len(chunk), N1)
            count += 1
        self.assertEqual(count, N2)


class Stuff(object):
    def divide(self, x, y, delay=0.2):
        time.sleep(delay)
        return x/y

    def iterate(self, N, maxDelay=0.2):
        for k in xrange(N):
            if maxDelay > 0:
                time.sleep(maxDelay*random.random())
            yield k

    def stringerator(self):
        def wait():
            t0 = time.time()
            while time.time() - t0 < 0.2:
                time.sleep(0.05)
            return time.time() - t0
        for k in xrange(5):
            time.sleep(0.02)
            yield wait()


class TestThreadLooper(TestCase):
    verbose = False

    def setUp(self):
        self.stuff = Stuff()
        self.resultList = []
        self.t = threads.ThreadLooper()

    def tearDown(self):
        return self.t.stop()
        
    def test_loop(self):
        self.assertTrue(self.t.threadRunning)
        self.t.callTuple = None
        self.t.event.set()
        return deferToDelay(0.2).addCallback(
            lambda _: self.assertFalse(self.t.threadRunning))
            
    @defer.inlineCallbacks
    def test_call_OK_once(self):
        status, result = yield self.t.call(
            self.stuff.divide, 10, 2, delay=0.3)
        self.assertEqual(status, 'r')
        self.assertEqual(result, 5)

    def _gotOne(self, sr):
        self.assertEqual(sr[0], 'r')
        self.resultList.append(sr[1])

    def _checkResult(self, null, expected):
        self.assertEqual(self.resultList, expected)
        
    def test_call_multi_OK(self):
        dList = []
        for x in (2, 4, 8, 10):
            d = self.t.call(
                self.stuff.divide, x, 2, delay=0.2*random.random())
            d.addCallback(self._gotOne)
            dList.append(d)
        return defer.DeferredList(dList).addCallback(
            self._checkResult, [1, 2, 4, 5])

    def test_call_doNext(self):
        dList = []
        for num, delay, doNext in (
                (3, 0.4, False), (6, 0.1, False), (12, 0.1, True)):
            d = self.t.call(
                self.stuff.divide, num, 3, delay=delay, doNext=doNext)
            d.addCallback(self._gotOne)
            dList.append(d)
        return defer.DeferredList(dList).addCallback(
            self._checkResult, [1, 4, 2])

    @defer.inlineCallbacks
    def test_call_error_once(self):
        status, result = yield self.t.call(self.stuff.divide, 1, 0)
        self.assertEqual(status, 'e')
        self.msg("Expected error message:", '-')
        self.msg(result)
        self.assertPattern(r'divide', result)
        self.assertPattern(r'[dD]ivi.+zero', result)

    @defer.inlineCallbacks
    def test_iterator_basic(self):
        for k in xrange(100):
            N = random.randrange(5, 20)
            self.msg("Repeat #{:d}, iterating {:d} times...", k+1, N)
            status, result = yield self.t.call(self.stuff.iterate, N, 0)
            self.assertEqual(status, 'i')
            resultList = []
            for d in result:
                item = yield d
                resultList.append(item)
            self.assertEqual(resultList, range(N))
    
    @defer.inlineCallbacks
    def test_iterator_fast(self):
        status, result = yield self.t.call(self.stuff.iterate, 10)
        self.assertEqual(status, 'i')
        dRegular = self.t.call(self.stuff.divide, 3.0, 2.0)
        resultList = []
        for d in result:
            item = yield d
            resultList.append(item)
        self.assertEqual(resultList, range(10))
        status, result = yield dRegular
        self.assertEqual(status, 'r')
        self.assertEqual(result, 1.5)

    @defer.inlineCallbacks
    def test_iterator_slow(self):
        status, result = yield self.t.call(self.stuff.stringerator)
        self.assertEqual(status, 'i')
        dRegular = self.t.call(self.stuff.divide, 3.0, 2.0)
        resultList = []
        for d in result:
            item = yield d
            resultList.append(item)
        self.assertEqual(len(resultList), 5)
        self.assertApproximates(
            sum(resultList), 1.0, 0.05)
        status, result = yield dRegular
        self.assertEqual(status, 'r')
        self.assertEqual(result, 1.5)
        
    def test_deferToThread_OK(self):
        def done(result):
            self.assertEqual(result, 5)
        def oops(failureObj):
            self.fail("Shouldn't have gotten here!")
        return self.t.deferToThread(
            self.stuff.divide, 10, 2).addCallbacks(done, oops)
        
    def test_deferToThread_error(self):
        def done(result):
            self.fail("Shouldn't have gotten here!")
        def oops(failureObj):
            self.assertPattern(r'[dD]ivi', failureObj.getErrorMessage())
        return self.t.deferToThread(
            self.stuff.divide, 1, 0).addCallbacks(done, oops)

    @defer.inlineCallbacks
    def test_deferToThread_iterator(self):
        dr = yield self.t.deferToThread(self.stuff.stringerator)
        self.msg("Call to iterator returned: {}", repr(dr))
        valueList = []
        for d in dr:
            value = yield d
            self.assertApproximates(value, 0.2, 0.05)
            valueList.append(value)
        self.assertEqual(len(valueList), 5)


        
