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

import time, random, threading
from twisted.internet import defer

from testbase import TestCase, util, errors, deferToDelay


class Picklable(object):
    classValue = 1.2

    def __init__(self):
        self.x = 0

    def foo(self, y):
        self.x += y

    def __eq__(self, other):
        return (
            self.classValue == other.classValue
            and
            self.x == other.x
        )


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


class TestInfo(TestCase):
    verbose = False

    def setUp(self):
        self.info = util.Info()
    
    def test_aboutCall(self):
        for pattern, f, args, kw in (
            ('[cC]allable!', None, (), {}),
            ('\.foo\(1\)', self.foo, (1,), {}),
        ):
            self.info.setCall(f, *args, **kw)
            self.assertPattern(pattern, self.info.aboutCall())

    def test_callTraceback(self):
        try:
            self.foo(0)
        except Exception as e:
            text = self.info.aboutException()
        self.msg(text)
        self.assertPattern('Exception ', text)
        self.assertPattern('[dD]ivi.+by zero', text)


class TestThreadLooper(TestCase):
    verbose = True

    def setUp(self):
        self.t = util.ThreadLooper()

    def tearDown(self):
        return self.t.stop()
        
    def test_loop(self):
        self.assertTrue(self.t.threadRunning)
        self.t.callTuple = None
        self.t.event.set()
        return deferToDelay(0.2).addCallback(
            lambda _: self.assertFalse(self.t.threadRunning))

    def _divide(self, x, y, delay=0.5):
        import time
        time.sleep(delay)
        return x/y

    @defer.inlineCallbacks
    def test_call_OK(self):
        status, result = yield self.t.call(self._divide, 10, 2, delay=1.0)
        self.assertEqual(status, 'r')
        self.assertEqual(result, 5)
        
        
        
