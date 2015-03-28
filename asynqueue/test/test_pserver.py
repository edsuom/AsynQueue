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
Unit tests for asynqueue.pserver
"""

from twisted.internet import defer
from twisted.python.failure import Failure

from testbase import TestCase, errors, iteration, pserver


class TestChunkyString(TestCase):
    verbose = False

    def test_basic(self):
        x = "0123456789" * 11111
        cs = pserver.ChunkyString(x)
        # Test with a smaller chunk size
        N = 1000
        cs.chunkSize = N
        y = ""
        count = 0
        for chunk in cs:
            self.assertLessEqual(len(chunk), N)
            y += chunk
            count += 1
        self.assertEqual(y, x)
        self.msg("Produced {:d} char string in {:d} iterations", len(x), count)


class BigObject(object):
    itemSize = 10000
    
    def __init__(self, N):
        self.N = N

    def setContents(self):
        Nsf = 0
        self.keys = []
        self.stuff = {}
        while Nsf < self.N:
            N = min([self.N-Nsf, self.itemSize])
            self.keys.append(Nsf)
            self.stuff[Nsf] = "X" * N
            Nsf += N
        
        
class TestTaskUniverse(TestCase):
    verbose = False

    def setUp(self):
        self.u = pserver.TaskUniverse()

    @defer.inlineCallbacks
    def test_pf(self):
        yield self.u.call(lambda x: 2*x, 0)
        pf = self.u.pf('xyz')
        self.assertIsInstance(pf, iteration.Prefetcherator)
        self.assertEqual(self.u.pf('xyz'), pf)
        self.assertNotEqual(self.u.pf('abc'), pf)

    def _generatorMethod(self, x, N=7):
        for y in xrange(N):
            yield x*y
        
    def test_handleIterator(self):
        response = {'ID': None}
        self.u._handleIterator(self._generatorMethod(10), response)
        self.assertEqual(response['status'], 'i')
        ID = response['result']
        pf = self.u.pf(ID)
        self.assertIsInstance(pf, iteration.Prefetcherator)
        
    def test_handlePickle_small(self):
        obj = [1, 2.0, "3"]
        response = {'ID': None}
        pr = pserver.o2p(obj)
        self.u._handlePickle(pr, response)
        self.assertEqual(response['status'], 'r')
        self.assertEqual(response['result'], pr)

    @defer.inlineCallbacks
    def test_handlePickle_large(self):
        response = {'ID': None}
        bo = BigObject(N=200000)
        bo.setContents()
        pr = pserver.o2p(bo)
        self.u._handlePickle(pr, response)
        self.assertEqual(response['status'], 'c')
        pf = self.u.pf(response['result'])
        self.assertIsInstance(pf, iteration.Prefetcherator)
        chunks = []
        for d in iteration.Deferator(pf):
            chunk = yield d
            chunks.append(chunk)
        reconBigObject = pserver.p2o("".join(chunks))
        self.assertEqual(reconBigObject.keys, bo.keys)
        self.assertEqual(reconBigObject.stuff, bo.stuff)

    def _xyDivide(self, x, y=2):
        return x/y
        
    @defer.inlineCallbacks
    def test_call_single(self):
        response = yield self.u.call(self._xyDivide, 5.0)
        self.assertIsInstance(response, dict)
        self.assertEqual(response['status'], 'r')
        self.assertEqual(response['result'], pserver.o2p(2.5))
        response = yield self.u.call(self._xyDivide, 5.0, y=5)
        self.assertEqual(response['status'], 'r')
        self.assertEqual(response['result'], pserver.o2p(1.0))

    

        
