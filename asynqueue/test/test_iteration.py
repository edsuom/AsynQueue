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

import random
from twisted.internet import defer

from testbase import TestCase, iteration, errors


generator = (2*x for x in range(10))

def generatorFunction(x):
    for y in xrange(3,7):
        yield x*y


class DeferredIterable(object):
    def __init__(self, x):
        self.x = x

    def next(self):
        d = iteration.deferToDelay(2*random.random())
        d.addCallback(lambda _: self.x.pop())
        return d
        

class IteratorGetter(object):
    def __init__(self, x):
        self.x = x
        self.dHistory = []

    def getNext(self, slowness=5):
        if self.x:
            x = self.x.pop()
            d = iteration.deferToDelay(slowness*random.random())
            d.addCallback(lambda _ : (x, True, len(self.x) > 0))
        else:
            d = defer.succeed((None, False, False))
        self.dHistory.append(d)
        return d


class TestDeferator(TestCase):
    verbose = True

    def test_isIterator(self):
        isIterator = iteration.Deferator.isIterator
        for unSuitable in (None, "xyz", (1,2), [3,4], {1:'a', 2:'b'}):
            self.assertFalse(isIterator(unSuitable))
        for suitable in (generator, generatorFunction):
            self.assertTrue(isIterator(suitable), repr(suitable))

    def test_repr(self):
        x = range(10)
        repFunction = repr(generatorFunction)
        df = iteration.Deferator(repFunction, lambda _: x.pop())
        rep = repr(df)
        self.msg(rep)
        self.assertPattern("Deferator wrapping", rep)

    @defer.inlineCallbacks
    def test_iterates(self):
        x = [5, 4, 3, 2, 1, 0]
        ig = IteratorGetter(x)
        df = iteration.Deferator(repr(ig), ig.getNext, slowness=3)
        for k, d in enumerate(df):
            value = yield d
            self.msg("Item #{:d}: {}", k+1, value)
            self.assertEqual(value, k)
    

class TestPrefetcherator(TestCase):
    verbose = True
    
    def test_setIterator(self):
        pf = iteration.Prefetcherator()
        for unSuitable in (None, "xyz", (1,2), [3,4], {1:'a', 2:'b'}):
            self.assertFalse(pf.setIterator(unSuitable))
        self.assertTrue(pf.setIterator(generatorFunction(4)))
        self.assertEqual(pf.lastFetch, (12, True))

    @defer.inlineCallbacks
    def test_setNextCallable(self):
        di = DeferredIterable([7, 13, 21])
        pf = iteration.Prefetcherator()
        ok = yield pf.setNextCallable(di.next)
        self.assertTrue(ok)
        self.assertEqual(pf.lastFetch, (21, True))
