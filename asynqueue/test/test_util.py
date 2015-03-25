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

from testbase import TestCase, util, errors


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


def stufferator(x):
    for y in xrange(10):
        yield x*y


class TestFunctions(TestCase):
    verbose = False

    def foo(self, x):
        return 2/x

    def test_callInfo(self):
        self.assertPattern('[cC]allable!', util.callInfo(None))
        self.assertPattern('\.foo\(1\)', util.callInfo(self.foo, 1))

    def test_callTraceback(self):
        try:
            self.foo(0)
        except Exception as e:
            text = util.callTraceback(self.foo)
        self.msg(text)
        self.assertPattern('\.foo\(\)', text)
        self.assertPattern('\.foo\(0\)', text)
        self.assertPattern('[dD]ivi.+by zero', text)

    def test_pickling(self):
        pObj = Picklable()
        pObj.foo(3.2)
        pObj.foo(1.2)
        objectList = [
            None, "Some text!", 37311, -1.37,
            Exception, pObj]
        for obj in objectList:
            pickleString = util.o2p(obj)
            self.assertIsInstance(pickleString, str)
            roundTrip = util.p2o(pickleString)
            self.assertEqual(obj, roundTrip)
        self.assertEqual(roundTrip.x, 4.4)
