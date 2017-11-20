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
Unit tests for asynqueue.process
"""

import sys, logging
from StringIO import StringIO
from time import time

import numpy as np

from twisted.internet import defer

import base, process
from testbase import blockingTask, Tasks, \
    TestHandler, TestCase, IterationConsumer


class TestProcessQueue(TestCase):
    def setUp(self):
        self.t = Tasks()

    def tearDown(self):
        return self.q.shutdown()
        
    def _bogusCall(self, **kw):
        if not hasattr(self, 'q'):
            self.q = process.ProcessQueue(1, **kw)
        return self.q.call(self.t._divideBy, 1, 0)
    
    @defer.inlineCallbacks
    def test_error_default(self):
        stderr = sys.stderr
        sys.stderr = StringIO()
        try:
            result = yield self._bogusCall()
            self.msg("Bogus call result: {}", result)
        except Exception as e:
            self.fail("Exception raised")
        self.assertIn("Exception 'integer division", result)
        self.assertIn("ERROR:", sys.stderr.getvalue())
        sys.stderr.close()
        sys.stderr = stderr

    @defer.inlineCallbacks
    def test_error_warn(self):
        stderr = sys.stderr
        sys.stderr = StringIO()
        handler = TestHandler(self.isVerbose())
        logging.getLogger('asynqueue').addHandler(handler)
        try:
            result = yield self._bogusCall(warn=True)
            self.msg("Bogus call result: {}", result)
        except Exception as e:
            self.fail("Exception raised")
        self.assertIn("Exception 'integer division", result)
        self.assertEqual(
            len(sys.stderr.getvalue()), 0,
            "STDERR not blank: '{}'".format(sys.stderr.getvalue()))
        self.assertGreater(len(handler.records), 0)
        for record in handler.records:
            if record.levelno == logging.ERROR:
                break
        else:
            self.fail("No ERROR record found in log handler")
        sys.stderr.close()
        sys.stderr = stderr

    @defer.inlineCallbacks
    def test_error_returnFailure(self):
        def failed(failureObj):
            self.assertIn('integer division', failureObj.getTraceback())
            self.failedAsExpected = True
        
        stderr = sys.stderr
        sys.stderr = StringIO()
        try:
            yield self._bogusCall(returnFailure=True).addErrback(failed)
        except Exception as e:
            self.fail("Exception raised")
        self.assertTrue(self.failedAsExpected)
        self.assertEqual(
            len(sys.stderr.getvalue()), 0,
            "STDERR not blank: '{}'".format(sys.stderr.getvalue()))
        sys.stderr.close()
        sys.stderr = stderr


class TestProcessWorker(TestCase):
    verbose = False
    
    def setUp(self):
        self.worker = process.ProcessWorker()
        self.queue = base.TaskQueue()
        self.queue.attachWorker(self.worker)

    def tearDown(self):
        return self.queue.shutdown()

    def checkStopped(self, null):
        self.failIf(self.worker.process.is_alive())
            
    def test_shutdown(self):
        d = self.queue.call(blockingTask, 0, delay=0.5)
        d.addCallback(lambda _: self.queue.shutdown())
        d.addCallback(self.checkStopped)
        return d

    def test_shutdownWithoutRunning(self):
        d = self.queue.shutdown()
        d.addCallback(self.checkStopped)
        return d

    def test_stop(self):
        d = self.queue.call(blockingTask, 0)
        d.addCallback(lambda _: self.worker.stop())
        d.addCallback(self.checkStopped)
        return d

    def test_oneTask(self):
        d = self.queue.call(blockingTask, 15)
        d.addCallback(self.failUnlessEqual, 30)
        return d

    def test_multipleWorkers(self):
        N = 20
        mutable = []

        def gotResult(result):
            self.msg("Task result: {}", result)
            mutable.append(result)

        def checkResults(null):
            self.failUnlessEqual(len(mutable), N)
            self.failUnlessEqual(
                sum(mutable),
                sum([2*x for x in xrange(N)]))

        # Create and attach two more workers, for a total of three
        for null in xrange(2):
            worker = process.ProcessWorker()
            self.queue.attachWorker(worker)
        dList = []
        for x in xrange(N):
            d = self.queue.call(blockingTask, x)
            d.addCallback(gotResult)
            dList.append(d)
        d = defer.DeferredList(dList)
        d.addCallback(checkResults)
        return d

    @defer.inlineCallbacks
    def test_iterator(self):
        N1, N2 = 50, 100
        from util import TestStuff
        stuff = TestStuff()
        stuff.setStuff(N1, N2)
        consumer = IterationConsumer(self.verbose)
        yield self.queue.call(stuff.stufferator, consumer=consumer)
        for chunk in consumer.data:
            self.assertEqual(len(chunk), N1)
        self.assertEqual(len(consumer.data), N2)

        
class TestProcessWorkerStats(TestCase):
    verbose = True
    
    def setUp(self):
        self.worker = process.ProcessWorker(callStats=True)
        self.queue = base.TaskQueue()
        self.queue.attachWorker(self.worker)

    def tearDown(self):
        return self.queue.shutdown()

    @defer.inlineCallbacks
    def test_queueStats(self):
        yield self.queue.shutdown()
        self.queue = process.ProcessQueue(2, callStats=True)
        yield self._runCall(blockingTask, 0, 10, None)
        statsFromQueue = yield self.queue.stats()
        self.assertEqual(len(statsFromQueue), 10)
        
    def test_000Calls(self):
        return self._runCallWithStats(
            blockingTask, 100, 200, 0).addCallback(self._showStats)

    def test_0100Calls(self):
        return self._runCallWithStats(
            blockingTask, 10, 50, 0.1).addCallback(self._showStats)
        
    def test_1000Calls(self):
        return self._runCallWithStats(
            blockingTask, 1, 5, 1).addCallback(self._showStats)

    def test_randomCalls(self):
        return self._runCallWithStats(
            blockingTask, 1, 50, None).addCallback(self._showStats)

    @defer.inlineCallbacks
    def _runCallWithStats(self, f, xMin, xMax, delay):
        dispatchTime = yield self._runCall(f, xMin, xMax, delay)
        stats = yield self.worker.stats()
        defer.returnValue((dispatchTime, stats))

    @defer.inlineCallbacks
    def _runCall(self, f, xMin, xMax, delay):
        dList = []
        t0 = time()
        for x in xrange(xMin, xMax):
            dList.append(self.queue.call(f, x, delay))
        yield defer.DeferredList(dList)
        dispatchTime = time() - t0
        defer.returnValue(dispatchTime)
        
    def _showStats(self, stuff):
        dispatchTime, stats = stuff
        x = np.asarray(stats)
        workerTime, processTime = [np.sum(x[:,k]) for k in (0,1)]
        self.msg("Total times", "-")
        self.msg(
            "Process:\t{:0.7f} seconds, {:0.1f}%",
            processTime, 100*processTime/dispatchTime)
        self.msg(
            "Worker:\t\t{:0.7f} seconds, {:0.1f}%",
            workerTime, 100*workerTime/dispatchTime)
        self.msg("Total:\t\t{:0.7f} seconds", dispatchTime, "-")
        # Compute worker-to-process overhead stats
        self.msg("Worker-to-process overhead (per call)", "-")
        diffs = 1000*(x[:,0] - x[:,1])
        for line in self._histogram(diffs):
            self.msg(line)
        mean = np.mean(diffs)
        self.msg("Mean: {:0.7f} ms", mean)

    def _histogram(self, x):
        distinct = []
        for value in x:
            if value not in distinct:
                distinct.append(value)
            if len(distinct) > 10:
                break
        N_bins = len(distinct)
        counts, bins = np.histogram(x, bins=N_bins, density=False)
        yield "|"
        for k, count in enumerate(counts):
            lower = bins[k]
            upper = bins[k+1]
            line = "| {:7.2f} to {:7.2f} : {}".format(
                lower, upper, "*"*count)
            yield line
        yield "|"

            

        
