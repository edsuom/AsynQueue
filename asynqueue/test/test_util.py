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
Unit tests for asynqueue.util
"""

import os, sys, gc, random, threading, time
from zope.interface import implements
from twisted.internet import defer
from twisted.internet.interfaces import IConsumer

from .. import util
from .testbase import deferToDelay, blockingTask, Picklable, TestCase


class TestFunctions(TestCase):
    verbose = False

    def test_pickling(self):
        pObj = Picklable()
        pObj.foo(3.2)
        pObj.foo(1.2)
        objectList = [None, "Some text!", 37311, -1.37, Exception, pObj]
        for obj in objectList:
            pickleString = util.o2p(obj)
            self.assertIsInstance(pickleString, bytes)
            roundTrip = util.p2o(pickleString)
            self.assertEqual(obj, roundTrip)
        self.assertEqual(roundTrip.x, 4.4)

    @defer.inlineCallbacks
    def test_kill(self):
        args = [sys.executable]*2
        args.extend(['-c', "while True: pass"])
        pid = os.spawnl(os.P_NOWAIT, *args)
        yield deferToDelay(0.1)
        wasAlive = yield util.killProcess(pid)
        self.assertTrue(wasAlive)
        self.assertFalse(os.path.exists(os.path.join("/proc", str(pid))))
        yield deferToDelay(0.1)
        wasAlive = yield util.killProcess(pid)
        self.assertFalse(wasAlive)
        

class TestDeferredTracker(TestCase):
    verbose = False

    def setUp(self):
        self._flag = False
        self._count = 0
        self.dt = util.DeferredTracker()

    def _setFlag(self):
        return defer.succeed(None).addCallback(
            lambda _: setattr(self, '_flag', True))
        
    def _slowStuff(self, N, delay=None, maxDelay=0.2, minDelay=0, ramp=False):
        def done(null, k):
            self._flag = False
            self._count -= 1
            return k
        dList = []
        for k in range(N):
            self._count += 1
            if delay is None:
                if ramp:
                    delay_k = minDelay + (maxDelay-minDelay)*k/N
                else: delay_k = minDelay + (maxDelay-minDelay)*random.random()
            else: delay_k = delay
            d = self.deferToDelay(delay_k).addCallback(done, k)
            dList.append(d)
        return dList
    
    @defer.inlineCallbacks
    def test_basic(self):
        # Nothing in there yet, immediate
        t0 = time.time()
        yield self.dt.deferToAll()
        self.assertAlmostEqual(time.time()-t0, 0, 3)
        # Put some in and wait for them
        for d in self._slowStuff(3):
            self.dt.put(d)
        self.assertEqual(self._count, 3)
        yield self.dt.deferToAll()
        self.assertEqual(self._count, 0)
        # Put some in with the same delay and defer to all
        t0 = time.time()
        for d in self._slowStuff(3, delay=0.5):
            self.dt.put(d)
        self.dt.put(self._setFlag())
        yield self.dt.deferToAll()
        self.assertFalse(self._flag)
        self.assertEqual(self._count, 0)
        self.assertGreater(time.time()-t0, 0.5)
        self.assertLess(time.time()-t0, 0.6)

    @defer.inlineCallbacks
    def test_deferToAll_multiple(self):
        # Put some in and wait for them
        for d in self._slowStuff(3):
            self.dt.put(d)
        # Wait for all, twice
        yield self.dt.deferToAll()
        t0 = time.time()
        yield self.dt.deferToAll()
        self.assertLess(time.time()-t0, 0.001)

    @defer.inlineCallbacks
    def test_deferToAny_basic(self):
        # Put some in and wait any for them to fire
        for d in self._slowStuff(10, minDelay=0.1, maxDelay=0.5):
            self.dt.put(d)
        # Wait for just one
        t0 = time.time()
        yield self.dt.deferToAny()
        elapsed = time.time()-t0
        self.assertGreater(elapsed, 0.1)
        self.assertLess(elapsed, 0.3)
        yield self.dt.deferToAll()

    @defer.inlineCallbacks
    def test_deferToAny_and_deferToAll(self):
        def justOne(null):
            OK.append(True)
            elapsed = time.time()-t0
            self.assertGreater(elapsed, 0.1)
            self.assertLess(elapsed, 0.3)

        OK = []
        # Put some in
        for d in self._slowStuff(10, minDelay=0.1, maxDelay=0.5):
            self.dt.put(d)
        # Wait for just one, and for any
        t0 = time.time()
        yield defer.DeferredList([
            self.dt.deferToAny().addCallback(justOne), self.dt.deferToAll()])
        self.assertTrue(OK)

    @defer.inlineCallbacks
    def test_deferUntilFewer(self):
        N = 100
        # Put some in
        for d in self._slowStuff(N, minDelay=0.0, maxDelay=1.0, ramp=True):
            self.dt.put(d)
        # Wait for there to be half as many
        t0 = time.time()
        yield self.dt.deferUntilFewer(N/2)
        elapsed = time.time()-t0
        self.assertGreater(elapsed, 0.5)
        self.assertLess(elapsed, 0.55)
        self.assertGreater(self.dt.dCount, N/2-7)
        self.assertLess(self.dt.dCount, N/2)
        yield self.dt.deferToAll()
        
    def test_memory(self):
        def doneDelaying(null, k):
            newCounts = gc.get_count()
            # Can't have more than 4 third-generation counts during
            # iteration than we started with
            self.assertTrue(newCounts[-1] < counts[-1]+5)
        def done(null):
            newCounts = gc.get_count()
            # Can't have more than 9 counts left after iteration than
            # we started with, for any generation. Should be able to
            # repeat this test (with -u) all day.
            for k in range(3):
                self.assertTrue(newCounts[k] < counts[k]+10)
        counts = gc.get_count()
        for k in range(1000):
            d = self.deferToDelay(0.1*random.random())
            d.addCallback(doneDelaying, k)
            self.dt.put(d)
        return self.dt.deferToAll().addCallback(done)
    
    @defer.inlineCallbacks
    def test_deferToAll_quitWaiting(self):
        # Put some in and supposedly wait for them
        for d in self._slowStuff(3, minDelay=0.4, maxDelay=0.6):
            self.dt.put(d)
        # Wait for all
        t0 = time.time()
        self.deferToDelay(0.2).addCallback(lambda _: self.dt.quitWaiting())
        yield self.dt.deferToAll()
        td = time.time()-t0
        self.assertWithinFivePercent(td, 0.22)

        
class TestCallRunner(TestCase):
    verbose = True
    
    def _divide(self, x, y):
        return x/y
    
    def test_withStats(self):
        runner = util.CallRunner(callStats=True)
        z = []
        for x in range(1000, 2000):
            result = runner((self._divide, (x, 2), {}))
            self.assertEqual(result[0], b'r')
            z.append(result[1])
        self.assertEqual(len(z), 1000)
        self.assertEqual(z[0], 500)
        callTimes = runner.callTimes
        self.assertEqual(len(callTimes), 1000)
        self.assertLess(max(callTimes), 1E-4)
        self.msg(
            "Call times range from {:f} to {:f} ms",
            1000*min(callTimes), 1000*max(callTimes))

    

