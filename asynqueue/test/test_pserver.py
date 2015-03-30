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
import sys, os, os.path

from twisted.internet import defer, utils, reactor, endpoints
from twisted.python.failure import Failure
from twisted.protocols import amp

from testbase import TestCase, deferToDelay, ProcessProtocol, \
    errors, iteration, pserver


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
        self.stuff = []
        while Nsf < self.N:
            N = min([self.N-Nsf, self.itemSize])
            self.stuff.append("X" * N)
            Nsf += N
        return self

    def getNext(self):
        if self.stuff:
            chunk = self.stuff.pop(0)
            return (chunk, True, len(self.stuff) > 0)
        return (None, False, False)
        
        
class TestTaskUniverse(TestCase):
    verbose = False

    def setUp(self):
        self.u = pserver.TaskUniverse()

    def tearDown(self):
        return self.u.shutdown()
        
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
        bo = BigObject(N=200000).setContents()
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
        self.assertEqual(reconBigObject.stuff, bo.stuff)

    def _xyDivide(self, x, y=2):
        return x/y
        
    @defer.inlineCallbacks
    def test_call_single(self):
        response = yield self.u.call(self._xyDivide, 5.0)
        self.assertIsInstance(response, dict)
        self.assertEqual(response['status'], 'r')
        self.assertEqual(response['result'], pserver.o2p(2.5))
        response = yield self.u.call(self._xyDivide, 0.0, y=1)
        self.assertEqual(response['status'], 'r')
        self.assertEqual(response['result'], pserver.o2p(0.0))

    @defer.inlineCallbacks
    def test_call_error(self):
        response = yield self.u.call(self._xyDivide, 1.0, y=0)
        self.assertIsInstance(response, dict)
        self.assertEqual(response['status'], 'e')
        self.assertPattern(r'[dD]ivi', response['result'])

    @defer.inlineCallbacks
    def test_call_multiple(self):
        def gotResponse(response):
            self.assertEqual(response['status'], 'r')
            resultList.append(float(pserver.p2o(response['result'])))
        
        dList = []
        resultList = []
        for x in xrange(5):
            d = self.u.call(self._xyDivide, float(x), y=1)
            d.addCallback(gotResponse)
            dList.append(d)
        yield defer.DeferredList(dList)
        self.assertEqual(resultList, [0.0, 1.0, 2.0, 3.0, 4.0])

    @defer.inlineCallbacks
    def test_getMore(self):
        N = 200000
        chunks = []
        ID = "testID"
        bo = BigObject(N).setContents()
        self.u.pfs[ID] = bo
        while True:
            response = yield self.u.getNext(ID)
            self.assertTrue(response['isValid'])
            chunks.append(pserver.p2o(response['value']))
            if not response['moreLeft']:
                break
        self.assertEqual(len("".join(chunks)), N)

    @defer.inlineCallbacks
    def test_shutdown(self):
        results = []
        d = self.u.call(
            deferToDelay, 0.5).addCallback(lambda _: results.append(None))
        yield self.u.shutdown()
        self.assertEqual(results, [None])
        

class TestTaskServerBasics(TestCase):
    verbose = True

    def setUp(self):
        self.ts = pserver.TaskServer()
        self.ts.u = pserver.TaskUniverse()

    def checkCallable(self, f):
        self.assertTrue(callable(f))
        
    def test_parseArg(self):
        self.checkCallable(
            self.ts._parseArg(pserver.o2p(pserver.TestStuff.divide)))
        self.checkCallable(
            self.ts._parseArg("asynqueue.pserver.divide"))

            
class TestTaskServerRemote(TestCase):
    verbose = False

    @defer.inlineCallbacks
    def tearDown(self):
        if hasattr(self, 'ap'):
            yield self.ap.callRemote(pserver.QuitRunning)
            yield self.ap.transport.loseConnection()
        yield self.pt.loseConnection()
        yield deferToDelay(0.5)
    
    def _startServer(self):
        def ready(stdout):
            self.assertEqual(stdout, "OK")
            self.msg("Task Server ready for connection")
            dest = endpoints.UNIXClientEndpoint(reactor, address)
            return endpoints.connectProtocol(
                dest, amp.AMP()).addCallback(connected)

        def connected(ap):
            self.ap = ap
            self.msg("Connected with AMP protocol {}", repr(ap))
            return ap

        address = os.path.expanduser(
            os.path.join("~", "test-pserver.sock"))
        args = [sys.executable, "-m", "asynqueue.pserver", address]
        pp = ProcessProtocol(self.verbose)
        self.pt = reactor.spawnProcess(pp, sys.executable, args)
        self.msg("Spawning Python interpreter {:d}", self.pt.pid)
        return pp.waitUntilReady().addCallback(ready)

    def test_start(self):
        def started(ap):
            self.assertIsInstance(ap, amp.AMP)
        return self._startServer().addCallback(started)

    @defer.inlineCallbacks
    def test_runTask_globalModule(self):
        ap = yield self._startServer()
        pargs = pserver.o2p((3.0, 2.0))
        response = yield ap.callRemote(
            pserver.RunTask,
            fn="asynqueue.pserver.divide", args=pargs, kw="")
        self.assertIsInstance(response, dict)
        self.msg(response['result'])
        self.assertEqual(response['status'], 'r')
        self.assertEqual(pserver.p2o(response['result']), 1.5)

    @defer.inlineCallbacks
    def test_runTask_namespace(self):
        ap = yield self._startServer()
        from asynqueue.pserver import TestStuff
        ts = TestStuff()
        response = yield ap.callRemote(
            pserver.SetNamespace, np=pserver.o2p(ts))
        self.assertEqual(response['status'], "OK")
        total = 0
        for x in xrange(5):
            total += x
            pargs = pserver.o2p((x,))
            response = yield ap.callRemote(
                pserver.RunTask,
                fn="accumulate", args=pargs, kw="")
            self.assertIsInstance(response, dict)
            self.msg(response['result'])
            self.assertEqual(response['status'], 'r')
            self.assertEqual(pserver.p2o(response['result']), total)
    
    @defer.inlineCallbacks
    def test_iterate(self):
        chunks = []
        N1, N2 = 200, 1000
        ap = yield self._startServer()
        from asynqueue.pserver import TestStuff
        ts = TestStuff().setStuff(N1, N2)
        response = yield ap.callRemote(
            pserver.SetNamespace, np=pserver.o2p(ts))
        self.msg("!SetNamespace: {}", response)
        self.assertEqual(response['status'], 'OK', response['status'])
        response = yield ap.callRemote(
            pserver.RunTask, fn="stufferator", args="", kw="")
        self.msg("!RunTask:stufferator: {}", response)
        self.assertEqual(response['status'], 'i', response['result'])
        ID = response['result']
        while True:
            response = yield ap.callRemote(
                pserver.GetMore, ID=ID)
            self.msg("!GetMore: {}", response)
            self.assertTrue(response['isValid'])
            chunks.append(pserver.p2o(response['value']))
            if not response['moreLeft']:
                break
        self.assertEqual(chunks, ts.stuff)

            
