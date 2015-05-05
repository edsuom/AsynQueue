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
Unit tests for asynqueue.util
"""

import gc, random, threading
from zope.interface import implements
from twisted.internet import defer
from twisted.internet.interfaces import IConsumer

import util
from testbase import deferToDelay, blockingTask, Picklable, TestCase


class TestFunctions(TestCase):
    verbose = False

    def test_pickling(self):
        pObj = Picklable()
        pObj.foo(3.2)
        pObj.foo(1.2)
        objectList = [None, "Some text!", 37311, -1.37, Exception, pObj]
        for obj in objectList:
            pickleString = util.o2p(obj)
            self.assertIsInstance(pickleString, str)
            roundTrip = util.p2o(pickleString)
            self.assertEqual(obj, roundTrip)
        self.assertEqual(roundTrip.x, 4.4)


class TestDeferredTracker(TestCase):
    verbose = False

    def setUp(self):
        self._flag = False
        self.dt = util.DeferredTracker()

    def _setFlag(self):
        return defer.succeed(None).addCallback(
            lambda _: setattr(self, '_flag', True))
        
    def _slowStuff(self, N, delay=None, maxDelay=0.2):
        def done(null, k):
            self._flag = False
            return k
        dList = []
        for k in xrange(N):
            if delay is None:
                delay = maxDelay*random.random()
            d = deferToDelay(delay).addCallback(done, k)
            dList.append(d)
        return dList
    
    @defer.inlineCallbacks
    def test_basic(self):
        # Nothing in there yet, immediate
        yield self.dt.deferToAll()
        yield self.dt.deferToLast()
        # Put some in and wait for them
        for d in self._slowStuff(3):
            self.dt.put(d)
        yield self.dt.deferToAll()
        self.assertEqual(len(self.dt.dList), 0)
        # Put some in with the same delay and defer to the last one
        for d in self._slowStuff(3, delay=0.5):
            self.dt.put(d)
        self.dt.put(self._setFlag())
        yield self.dt.deferToLast()
        self.assertTrue(self._flag)
        self.assertGreater(len(self.dt.dList), 0)
        yield self.dt.deferToAll()
        self.assertFalse(self._flag)
        self.assertEqual(len(self.dt.dList), 0)

    @defer.inlineCallbacks
    def test_deferToAll_multiple(self):
        # Put some in and wait for them
        for d in self._slowStuff(3):
            self.dt.put(d)
        # Wait for all, twice
        yield self.dt.deferToAll()
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
            for k in xrange(3):
                self.assertTrue(newCounts[k] < counts[k]+10)
        counts = gc.get_count()
        for k in xrange(1000):
            d = deferToDelay(0.1*random.random())
            d.addCallback(doneDelaying, k)
            self.dt.put(d)
        return self.dt.deferToAll().addCallback(done)

        
class TestCallRunner(TestCase):
    verbose = True
    
    def _divide(self, x, y):
        return x/y
    
    def test_withStats(self):
        runner = util.CallRunner(callStats=True)
        z = []
        for x in xrange(1000, 2000):
            result = runner((self._divide, (x, 2), {}))
            self.assertEqual(result[0], 'r')
            z.append(result[1])
        self.assertEqual(len(z), 1000)
        self.assertEqual(z[0], 500)
        callTimes = runner.callTimes
        self.assertEqual(len(callTimes), 1000)
        self.assertLess(max(callTimes), 1E-4)
        self.msg(
            "Call times range from {:f} to {:f} ms",
            1000*min(callTimes), 1000*max(callTimes))

    

