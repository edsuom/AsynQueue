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
Unit tests for asynqueue.workers
"""

import sys, os.path, time, random
from copy import copy

from twisted.internet import defer, reactor, endpoints
from twisted.protocols import amp

from asynqueue import base, iteration, util, misc, wire
from asynqueue.util import TestStuff, o2p, p2o
from asynqueue.test.testbase import deferToDelay, TestCase


class Test_WireWorker(TestCase):
    verbose = False

    def setUp(self):
        self.queue = base.TaskQueue()
        # This is needed to have a fully qualified wwu
        self.wwu = misc.TestUniverse()
        self.wm = wire.ServerManager('asynqueue.misc.TestUniverse')

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.queue.shutdown()
        yield self.wm.done()

    @defer.inlineCallbacks
    def newServer(self, stdio=False):
        description = self.wm.newSocket()
        result = yield self.wm.spawn(description, stdio=stdio)
        self.pid = result.pid if hasattr(result, 'pid') else result
        self.worker = wire.WireWorker(self.wwu, description)
        yield self.queue.attachWorker(self.worker)
    
    @defer.inlineCallbacks
    def test_basic(self):
        yield self.newServer(True)
        result = yield self.queue.call('add', 1, 2)
        self.assertEqual(result, 3)

    @defer.inlineCallbacks
    def test_afterDisconnect(self):
        yield self.newServer(True)
        yield util.killProcess(self.pid)
        d = self.queue.call('add', 2, 3)
        yield self.newServer(True)
        result = yield d
        self.assertEqual(result, 5)

    @defer.inlineCallbacks
    def test_iterate(self):
        yield self.newServer(True)
        chunks = []
        N1, N2 = 20, 10
        yield self.queue.call('setStuff', N1, N2)
        stuff = yield self.queue.call('getStuff')
        stuffSize = yield self.queue.call('stuffSize')
        self.assertEqual(len(stuff), stuffSize)
        dr = yield self.queue.call('stufferator')
        self.msg("Call to 'stufferator' returned '{}'", dr)
        for d in dr:
            chunk = yield d
            chunks.append(chunk)
        self.assertEqual(chunks, stuff)

    @defer.inlineCallbacks
    def test_basic_not_stdio(self):
        yield self.newServer()
        result = yield self.queue.call('add', 1, 2)
        self.assertEqual(result, 3)
        

class Test_ChunkyString(TestCase):
    verbose = False

    def test_basic(self):
        x = "0123456789" * 11111
        cs = wire.ChunkyString(x)
        # Test with a smaller chunk size
        N = 1234
        cs.chunkSize = N
        y = ""
        count = 0
        for chunk in cs:
            self.assertLessEqual(len(chunk), N)
            y += chunk
            count += 1
        self.assertEqual(y, x)
        self.msg(
            "Produced {:d} char string in {:d} iterations", len(x), count)


class BigObject(object):
    itemSize = 10000
    
    def __init__(self, N):
        self.N = N

    def getContents(self):
        return b"".join(self.stuff)
        
    def setContents(self):
        Nsf = 0
        self.stuff = []
        characters = "XO-I"
        while Nsf < self.N:
            N = min([self.N-Nsf, self.itemSize])
            stuff = "".join([
                characters[random.randint(0, len(characters)-1)]
                for k in range(N)])
            self.stuff.append(stuff.encode())
            Nsf += N
        return self

    def iter(self):
        return self

    def __next__(self):
        if self.stuff:
            return self.stuff.pop(0)
        raise StopIteration

        
class Test_WireRunner(TestCase):
    verbose = False

    def setUp(self):
        self.wr = wire.WireRunner()
        self.tm = misc.TestMethods()

    def tearDown(self):
        return self.wr.shutdown()
        
    @defer.inlineCallbacks
    def test_call_single(self):
        response = yield self.wr.call(self.tm.divide, 5.0, 2)
        self.assertIsInstance(response, dict)
        self.assertEqual(response['status'], b'r')
        self.assertEqual(response['result'], o2p(2.5))

    @defer.inlineCallbacks
    def test_call_chunked(self):
        N1, N2 = 10, 100000
        response = yield self.wr.call(self.tm.setStuff, N1, N2)
        self.assertIsInstance(response, dict)
        response = yield self.wr.call(self.tm.getStuff)
        self.assertIsInstance(response, dict)
        self.assertEqual(response['status'], b'c')
        ID = response['result']
        self.assertIsInstance(ID, bytes)
        chunks = []
        while True:
            response = yield self.wr.getNext(ID)
            if response['isValid']:
                self.assertTrue(response['isRaw'])
                chunks.append(response['value'])
            else:
                break
        stuff = p2o(b"".join(chunks))
        self.tm.setStuff(N1, N2)
        expectedStuff = self.tm.getStuff()
        self.assertEqual(stuff, expectedStuff)
        
    @defer.inlineCallbacks
    def test_call_error(self):
        response = yield self.wr.call(self.tm.divide, 1.0, 0)
        self.assertIsInstance(response, dict)
        self.assertEqual(response['status'], b'e')
        self.assertPattern(r'[dD]ivi', response['result'])

    @defer.inlineCallbacks
    def test_call_multiple(self):
        def gotResponse(response):
            self.assertEqual(response['status'], b'r')
            resultList.append(float(p2o(response['result'])))
        dList = []
        resultList = []
        for x in range(5):
            d = self.wr.call(self.tm.divide, float(x), 1)
            d.addCallback(gotResponse)
            dList.append(d)
        yield defer.DeferredList(dList)
        self.assertEqual(resultList, [0.0, 1.0, 2.0, 3.0, 4.0])

    @defer.inlineCallbacks
    def test_getNext(self):
        N = 200000
        chunks = []
        ID = "testID"
        bo = BigObject(N).setContents()
        stuff = bo.getContents()
        self.wr.iterators[ID] = bo
        k = 1
        while True:
            response = yield self.wr.getNext(ID)
            if not response['isValid']:
                self.assertEqual(response['value'], b"")
                break
            self.msg(
                "Response #{:d}: {:d} chars",
                k, len(response['value']))
            k += 1
            self.assertTrue(response['isRaw'])
            chunks.append(response['value'])
        joined = b"".join(chunks)
        self.assertEqual(joined, stuff)
    
    @defer.inlineCallbacks
    def test_call_iterator(self):
        N1, N2 = 20, 10
        yield self.wr.call(self.tm.setStuff, N1, N2)
        response = yield self.wr.call(self.tm.getStuff)
        stuff = p2o(response['result'])
        self.assertEqual(len(stuff), N1)
        response = yield self.wr.call(self.tm.stufferator)
        self.assertEqual(response['status'], b'i')
        ID = response['result']
        self.assertIn(ID, self.wr.iterators)
        self.assertEqual(
            type(self.wr.iterators[ID]),
            type(self.tm.stufferator()))
        for k in range(N1+1):
            response = yield self.wr.getNext(ID)
            chunk = response['value']
            if k < N1:
                self.assertTrue(response['isValid'])
                self.assertTrue(response['isRaw'])
                self.assertEqual(chunk, stuff[k])
            else:
                self.assertFalse(response['isValid'])

    @defer.inlineCallbacks
    def test_shutdown(self):
        results = []
        d = self.wr.call(
            deferToDelay, 0.5).addCallback(lambda _: results.append(None))
        yield self.wr.shutdown()
        self.assertEqual(results, [None])
