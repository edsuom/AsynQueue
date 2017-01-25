# Twisted Goodies:
# Miscellaneous add-ons and improvements to the separately maintained and
# licensed Twisted (TM) asynchronous framework. Permission to use the name was
# graciously granted by Twisted Matrix Laboratories, http://twistedmatrix.com.
#
# Copyright (C) 2006-2007 by Edwin A. Suominen, http://www.eepatents.com
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
Intelligent import, Mock objects, and an improved TestCase for AsynQueue
"""

import re, sys, os.path, time, random

from zope.interface import implements
from twisted.internet import reactor, defer, task
from twisted.internet.interfaces import IProducer, IConsumer

from twisted.trial import unittest

from asynqueue.info import Info
from asynqueue.threads import deferToThread
from asynqueue import iteration
from asynqueue.workers import AsyncWorker


VERBOSE = False


def deferToDelay(delay):
    d = defer.Deferred()
    reactor.callLater(delay, d.callback, None)
    return d

def blockingTask(x, delay=None):
    if delay is None:
        delay = random.uniform(0.01, 0.2)
    if delay:
        time.sleep(delay)
    return 2*x
    

class MsgBase(object):
    """
    A mixin for providing a convenient message method.
    """
    def isVerbose(self):
        if hasattr(self, 'verbose'):
            return self.verbose
        if 'VERBOSE' in globals():
            return VERBOSE
        return False
    
    def verboserator(self):
        if self.isVerbose():
            yield None

    def msg(self, proto, *args):
        for null in self.verboserator():
            if not hasattr(self, 'msgAlready'):
                proto = "\n" + proto
                self.msgAlready = True
            if args and args[-1] == "-":
                args = args[:-1]
                proto += "\n{}".format("-"*40)
            print proto.format(*args)


class DeferredIterable(object):
    def __init__(self, x):
        self.x = x

    def __iter__(self):
        return self
        
    def next(self):
        d = iteration.deferToDelay(0.3*random.random())
        d.addCallback(lambda _: self.x.pop(0))
        return d


class ProcessProtocol(MsgBase):
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.d = defer.Deferred()
    def waitUntilReady(self):
        return self.d
    def makeConnection(self, process):
        pass
    def childDataReceived(self, childFD, data):
        data = data.strip()
        if childFD == 2:
            self.msg(
                "ERROR on pserver:\n{}\n{}\n{}\n",
                "-"*40, data, "-"*40)
        else:
            self.msg("Data on FD {:d}: '{}'", childFD, data)
        if childFD == 1 and not self.d.called:
            self.d.callback(data)
    def childConnectionLost(self, childFD):
        self.msg("Connection Lost")
    def processExited(self, reason):
        self.msg("Process Exited")
    def processEnded(self, reason):
        self.msg("Process Ended")


class RangeProducer(object):
    """
    Produces an integer range of values like C{xrange}.

    Fires a C{Deferred} accessible via my I{d} attribute when the
    range has been produced.
    """
    implements(IProducer)

    def __init__(self, consumer, N, streaming, minInterval, maxInterval=None):
        """
        Constructs an instance of me to produce a range of I{N} integer
        values with the specified I{interval} between them.
        """
        if not IConsumer.providedBy(consumer):
            raise errors.ImplementationError(
                "Object {} isn't a consumer".format(repr(consumer)))
        self.produce = False
        self.minInterval = minInterval
        self.maxInterval = maxInterval
        self.consumer = consumer
        self.k, self.N = 0, N
        self.streaming = streaming
        self.t0 = time.time()
        self.d = defer.Deferred()
        if streaming:
            self.resumeProducing()
        consumer.registerProducer(self, streaming)

    @property
    def interval(self):
        if self.maxInterval is None:
            return self.minInterval
        return random.uniform(self.minInterval, self.maxInterval)
        
    def setNextCall(self):
        if not hasattr(self, 'dc') or not self.dc.active():
            self.dc = reactor.callLater(self.interval, self.nextValue)

    def stopProducing(self):
        self.produce = None

    def pauseProducing(self):
        if self.produce:
            self.produce = False

    def resumeProducing(self):
        if self.produce == False:
            self.produce = True
            self.setNextCall()

    def nextValue(self):
        if self.produce == None:
            if not self.d.called:
                self.d.callback(time.time() - self.t0)
            return
        self.setNextCall()
        if self.produce == False:
            return
        if self.k < self.N:
            self.consumer.write(self.k)
            self.k += 1
        if self.k == self.N:
            self.stopProducing()
            self.consumer.unregisterProducer()


class RangeWriter(object):
    """
    Writes an integer range of values like C{xrange} to a file-like
    object I{fh} and then closes it.

    Fires a C{Deferred} accessible via my I{d} attribute when the
    range has been written.
    """
    implements(IProducer)

    def __init__(self, fh, N, minInterval, maxInterval=None):
        """
        Constructs an instance of me to produce a range of I{N} integer
        values with the specified I{interval} between them.
        """
        self.fh = fh
        self.produce = True
        self.minInterval = minInterval
        self.maxInterval = maxInterval
        self.k, self.N = 0, N
        self.t0 = time.time()
        self.d = defer.Deferred()
        self.setNextCall()

    @property
    def interval(self):
        if self.maxInterval is None:
            return self.minInterval
        return random.uniform(self.minInterval, self.maxInterval)
        
    def setNextCall(self):
        if not hasattr(self, 'dc') or not self.dc.active():
            self.dc = reactor.callLater(self.interval, self.nextValue)

    def nextValue(self):
        if not self.produce:
            self.fh.close()
            if not self.d.called:
                self.d.callback(time.time() - self.t0)
            return
        self.setNextCall()
        if self.k < self.N:
            self.fh.write(self.k)
            self.k += 1
        if self.k == self.N:
            self.produce = False


class FakeFile(MsgBase):
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.data = []

    def write(self, data):
        self.data.append(data)
        if isinstance(data, (str, list, tuple)):
            self.msg("Data received, len: {:d}", len(data))
        else:
            self.msg("Data received: '{}'", data)

    def close(self):
        self.msg("Closed")

        
class IterationConsumer(MsgBase):
    implements(IConsumer)
    
    def __init__(self, verbose=False, stopAfter=None):
        self.verbose = verbose
        self.producer = None
        self.stopAfter = stopAfter

    def registerProducer(self, producer, streaming):
        if self.producer:
            raise RuntimeError()
        self.producer = producer
        producer.registerConsumer(self)
        self.data = []
        self.msg(
            "Registered with producer {}. Streaming: {}",
            repr(producer), repr(streaming))

    def unregisterProducer(self):
        self.producer = None
        self.msg("Producer unregistered")

    def write(self, data):
        self.data.append(data)
        if isinstance(data, (str, list, tuple)):
            self.msg("Data received, len: {:d}", len(data))
        else:
            self.msg("Data received: '{}'", data)
        if self.stopAfter and len(self.data) == self.stopAfter:
            self.producer.stopProducing()


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


class MockWireWorker(AsyncWorker):
    def __init__(self, *args, **kw):
        kw['series'] = ['mcm']
        super(MockWireWorker, self).__init__(*args, **kw)
        from wire import MandelbrotWorkerUniverse
        self.mwu = MandelbrotWorkerUniverse()

    def run(self, task):
        def ready(null):
            return deferToThread(
                f, *args, **kw).addCallbacks(done, oops)

        def done(result):
            if not raw and iteration.isIterator(result):
                try:
                    result = iteration.Deferator(result)
                except:
                    result = []
                else:
                    if consumer:
                        result = iteration.IterationProducer(result, consumer)
                status = 'i'
            else:
                status = 'r'
            # Hangs if release is done after the task callback
            self.dLock.release()
            print "\n{} --> {}".format(
                self.info.setCall(f, args, kw).aboutCall(),
                (status, result))
            task.callback((status, result))

        def oops(failureObj):
            text = self.info.setCall(f, args, kw).aboutFailure(failureObj)
            print "\nOOPS: {}".format(text)
            task.callback(('e', text))
        
        fName, args, kw = task.callTuple
        f = getattr(self.mwu, fName)
        task.callTuple = f, args, kw
        raw = kw.pop('raw', None)
        if raw is None:
            raw = self.raw
        consumer = kw.pop('consumer', None)
        vip = (kw.pop('doNext', False) or task.priority <= -20)
        return self.dLock.acquire(vip).addCallback(ready)
        

class TestCase(MsgBase, unittest.TestCase):
    """
    Slightly improved TestCase
    """
    # Nothing should take longer than 10 seconds, and often problems
    # aren't apparent until the timeout stops the test.
    timeout = 10

    def doCleanups(self):
        if hasattr(self, 'msgAlready'):
            del self.msgAlready
        return super(TestCase, self).doCleanups()

    def multiplerator(self, N, expected):
        def check(null):
            self.assertEqual(resultList, expected)
            del self.d
        
        dList = []
        resultList = []
        for k in xrange(N):
            yield k
            self.d.addCallback(resultList.append)
            dList.append(self.d)
        self.dm = defer.DeferredList(dList).addCallback(check)
            
    def checkOccurrences(self, pattern, text, number):
        occurrences = len(re.findall(pattern, text))
        if occurrences != number:
            info = \
                u"Expected {:d} occurrences, not {:d}, " +\
                u"of '{}' in\n-----\n{}\n-----\n"
            info = info.format(number, occurrences, pattern, text)
            self.assertEqual(occurrences, number, info)
    
    def checkBegins(self, pattern, text):
        pattern = r"^\s*%s" % (pattern,)
        self.assertTrue(bool(re.match(pattern, text)))

    def checkProducesFile(self, fileName, executable, *args, **kw):
        def done(result):
            self.assertTrue(
                os.path.exists(producedFile),
                "No file '{}' was produced.".format(producedFile))
            os.remove(producedFile)
            return result
        producedFile = fileName
        if os.path.exists(producedFile):
            os.remove(producedFile)
        return defer.maybeDeferred(executable, *args, **kw).addCallback(done)

    def runerator(self, executable, *args, **kw):
        return Runerator(self, executable, *args, **kw)

    def assertPattern(self, pattern, text):
        proto = "Pattern '{}' not in '{}'"
        if '\n' not in pattern:
            text = re.sub(r'\s*\n\s*', '', text)
        if isinstance(text, unicode):
            # What a pain unicode is...
            proto = unicode(proto)
        self.assertTrue(
            bool(re.search(pattern, text)),
            proto.format(pattern, text))

    def assertStringsEqual(self, a, b, msg=""):
        N_seg = 20
        def segment(x):
            k0 = max([0, k-N_seg])
            k1 = min([k+N_seg, len(x)])
            return "{}-!{}!-{}".format(x[k0:k], x[k], x[k+1:k1])
        
        for k, char in enumerate(a):
            if char != b[k]:
                s1 = segment(a)
                s2 = segment(b)
                msg += "\nFrom #1: '{}'\nFrom #2: '{}'".format(s1, s2)
                self.fail(msg)
