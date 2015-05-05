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
Unit tests for asynqueue.process
"""

from time import time

import numpy as np

from twisted.internet import defer

import base, process
from testbase import blockingTask, TestCase, IterationConsumer


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

            

        
