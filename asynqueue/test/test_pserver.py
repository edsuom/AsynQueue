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


class TestTaskUniverse(TestCase):
    verbose = False

    def setUp(self):
        self.u = pserver.TaskUniverse()

    def test_oops(self):
        self.u.response = {}
        failureObj = Failure(Exception("Test exception"))
        self.u._oops(failureObj)
        self.assertEqual(self.u.response['status'], 'e')
        self.assertPattern(r'Test exception', self.u.response['result'])

    @defer.inlineCallbacks
    def test_pf(self):
        yield self.u.call(lambda x: 2*x, 0)
        pf = self.u._pf()
        self.assertIsInstance(pf, iteration.Prefetcherator)
        self.assertEqual(self.u._pf(), pf)

    def _generatorMethod(self, x, N=7):
        for y in xrange(N):
            yield x*y
        
    def test_handleIterator(self):
        self.u.response = {}
        self.u._handleIterator(self._generatorMethod(10))
        self.assertEqual(self.u.response['status'], 'i')
        ID = self.u.response['result']
        pf = self.u._pf(ID)
        self.assertIsInstance(pf, iteration.Prefetcherator)
        
    def test_handlePickle_small(self):
        obj = [1, 2.0, "3"]
        self.u.response = {}
        pr = pserver.o2p(obj)
        self.u._handlePickle(pr)
        self.assertEqual(self.u.response['status'], 'r')
        self.assertEqual(self.u.response['result'], pr)
