# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker/manager interface.
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
Unit tests for asynqueue.threads
"""

import time, random, threading
from contextlib import contextmanager
from twisted.internet import defer, reactor

from util import TestStuff
import base, tasks, iteration, threads
from testbase import deferToDelay, RangeProducer, RangeWriter, \
    IterationConsumer, TestCase


class TaskMixin:
    def _producterator(self, x, N=7):
        for y in xrange(N):
            yield x*y

    def _blockingTask(self, x, maxTime=0.2):
        delay = random.uniform(0.0, maxTime)
        self.msg(
            "Running {:f} sec. task in thread {}",
            delay, threading.currentThread().getName())
        time.sleep(delay)
        return 2*x
            
    def _blockingIteratorUser(self, iterator, maxTime=0.2):
        self.values = []
        for x in iterator:
            # Doesn't this just seem rude after using Twisted a while?
            self.values.append(self._blockingTask(x, maxTime))
        return self.values


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


class TestConsumerator(TaskMixin, TestCase):
    verbose = False

    def setUp(self):
        self.q = threads.ThreadQueue()
        self.c = threads.Consumerator()

    def tearDown(self):
        return self.q.shutdown()
        
    @defer.inlineCallbacks
    def test_withPushProducer(self):
        N = 10
        totalTime = 2.0
        producer = RangeProducer(self.c, N, True, totalTime/N)
        values = yield self.q.call(self._blockingIteratorUser, self.c)
        timeSpent = yield producer.d
        self.msg(
            "Consumed {:d} iterations in {:f} seconds",
            len(values), timeSpent)
        self.assertEqual(len(values), N)
        self.assertEqual(values, range(0, 2*N, 2))
        self.assertAlmostEqual(timeSpent, 1.1*totalTime, 2)
        yield self.c.deferUntilDone()

    @defer.inlineCallbacks
    def test_withPullProducer(self):
        N = 20
        totalTime = 2.0
        producer = RangeProducer(self.c, N, False, totalTime/N)
        values = yield self.q.call(self._blockingIteratorUser, self.c)
        timeSpent = yield producer.d
        self.msg(
            "Consumed {:d} iterations in {:f} seconds",
            len(values), timeSpent)
        self.assertEqual(len(values), N)
        self.assertEqual(values, range(0, 2*N, 2))
        self.assertAlmostEqual(timeSpent, 1.05*totalTime, 2)
        yield self.c.deferUntilDone()
        
    @defer.inlineCallbacks
    def test_randomTiming(self):
        N = 400
        totalTime = 2.0
        producer = RangeProducer(self.c, N, True, totalTime/(4*N), totalTime/N)
        values = yield self.q.call(self._blockingIteratorUser, self.c, 0.03)
        yield self.c.deferUntilDone()
        self.assertEqual(len(values), N)
        self.assertEqual(values, range(0, 2*N, 2))
        yield producer.d


class TestFilerator(TaskMixin, TestCase):
    verbose = False

    def setUp(self):
        self.q = threads.TaskQueue()
        for series in ('writer', 'iterator'):
            worker = threads.ThreadWorker(series=[series])
            self.q.attachWorker(worker)
        self.f = threads.Filerator()

    def tearDown(self):
        return self.q.shutdown()
        
    @defer.inlineCallbacks
    def test_inMainThread(self):
        N = 10
        totalTime = 2.0
        writer = RangeWriter(self.f, N, totalTime/N)
        values = yield self.q.call(
            self._blockingIteratorUser, self.f, series='iterator')
        timeSpent = yield writer.d
        self.msg(
            "Consumed {:d} iterations in {:f} seconds",
            len(values), timeSpent)
        self.assertEqual(len(values), N)
        self.assertEqual(values, range(0, 2*N, 2))
        self.assertAlmostEqual(timeSpent, 1.1*totalTime, 2)
        yield self.f.deferUntilDone()

    @defer.inlineCallbacks
    def test_inBlockingThread(self):
        def blockingWriter(interval):
            t0 = time.time()
            for x in xrange(N):
                self.f.write(x)
                time.sleep(interval)
            self.f.close()
            return time.time() - t0
        N = 10
        totalTime = 2.0
        d1 = self.q.call(blockingWriter, totalTime/N, series='writer')
        values = yield self.q.call(
            self._blockingIteratorUser, self.f, series='iterator')
        timeSpent = yield d1
        self.msg(
            "Consumed {:d} iterations in {:f} seconds",
            len(values), timeSpent)
        self.assertEqual(len(values), N)
        self.assertEqual(values, range(0, 2*N, 2))
        self.assertAlmostEqual(timeSpent, totalTime, 2)
        yield self.f.deferUntilDone()

        
class TestOrderedItemProducer(TaskMixin, TestCase):
    verbose = False

    def setUp(self):
        self.p = threads.OrderedItemProducer()

    def fb(self, i):
        result = []
        for item in i:
            result.append(item)
        return result
        
    def fp(self, x, delay):
        return deferToDelay(delay).addCallback(lambda _: x)

    def test_produceItem(self):
        def started(null):
            return self.p.produceItem(
                self.fp, value, 0.1).addCallback(produced)
        def produced(item):
            self.assertEqual(item, value)
            return self.p.stop().addCallback(stopped)
        def stopped(returnValue):
            self.assertEqual(returnValue, [value])
        value = 15
        return self.p.start(self.fb).addCallback(started)
        
    @defer.inlineCallbacks
    def test_oneAtATime(self):
        pDelay = 0.02
        bDelay = 0.04
        yield self.p.start(self._blockingIteratorUser, maxTime=bDelay)
        inputs2x = []
        for x in xrange(100):
            delay = random.uniform(0, pDelay)
            item = yield self.p.produceItem(self.fp, x, delay)
            inputs2x.append(2*item)
        outputs = yield self.p.stop()
        self.assertEqual(outputs, inputs2x)

    @defer.inlineCallbacks
    def test_allAtOnce(self):
        def produced(item, k):
            inputs2x.append((k, 2*item))
        pDelay = 0.02
        bDelay = 0.04
        yield self.p.start(self._blockingIteratorUser, maxTime=bDelay)
        inputs2x = []
        for x in xrange(100):
            delay = random.uniform(0, pDelay)
            self.p.produceItem(
                self.fp, x, delay).addCallback(produced, x)
        outputs = yield self.p.stop()
        inputs2x.sort(key=lambda x: x[0])
        self.assertEqual(outputs, [x[1] for x in inputs2x])

                                   
                                   
