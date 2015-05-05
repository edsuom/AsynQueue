# Twisted Goodies:
# Miscellaneous add-ons and improvements to the separately maintained and
# licensed Twisted (TM) asynchronous framework. Permission to use the name was
# graciously granted by Twisted Matrix Laboratories, http://twistedmatrix.com.
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
Intelligent import, Mock objects, and an improved TestCase for AsynQueue
"""

import re, sys, os.path, time, random

from zope.interface import implements
from twisted.internet import reactor, defer
from twisted.internet.interfaces import IConsumer

from twisted.trial import unittest

from info import Info
from interfaces import IWorker
import iteration


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
        if isinstance(data, (list, tuple)):
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

        
class MockTask(object):
    def __init__(self, f, args, kw, priority, series, timeout=None):
        self.ran = False
        self.callTuple = (f, args, kw)
        self.priority = priority
        self.series = series
        self.d = defer.Deferred()
    
    def __cmp__(self, other):
        if other is None:
            return -1
        return cmp(self.priority, other.priority)

    def __str__(self):
        return str(self.callTuple[0])

    def startTimer(self):
        pass


class MockWorker(MsgBase):
    implements(IWorker)

    cQualified = []

    def __init__(self, runDelay=0.0, verbose=False):
        self.runDelay = runDelay
        self.verbose = verbose
        self.ran = []
        self.isShutdown = False
        self.iQualified = []
        self.info = Info()

    def setResignator(self, callableObject):
        pass

    def run(self, task):
        def ran(result, d):
            self.msg("Done with {}", repr(task))
            d.callback(None)
            return result

        self.msg("Running {}", repr(task), "-")
        self.task = task
        self.delayedCall = reactor.callLater(
            self.runDelay, self._reallyRun)
        d = defer.Deferred()
        task.d.addCallback(ran, d)
        return d
    
    def _reallyRun(self):
        f, args, kw = self.task.callTuple
        consumer = kw.pop('consumer', None)
        try:
            result = f(*args, **kw)
        except:
            status = 'e'
            result = self.info.setCall(f, args, kw).aboutException()
        else:
            status = 'r'
        self.ran.append(self.task)
        if iteration.Deferator.isIterator(result):
            status = 'i'
            try:
                result = iteration.Deferator(result)
            except:
                result = []
            else:
                if consumer:
                    result = iteration.IterationProducer(result, consumer)
        self.msg(
            "Worker {} ran {} ->\n {}: {}",
            getattr(self, 'ID', 0), str(self.task), status, result)
        self.task.d.callback((status, result))

    def stop(self):
        self.isShutdown = True
        self.msg("Shutting down worker {}", self)
        d = getattr(getattr(self, 'task', None), 'd', None)
        if d is None or d.called:
            d_shutdown = defer.succeed(None)
        else:
            d_shutdown = defer.Deferred()
            d.chainDeferred(d_shutdown)
        return d_shutdown

    def crash(self):
        delayedCall = getattr(self, 'delayedCall', None)
        if delayedCall and delayedCall.active():
            delayedCall.cancel()
            return [self.task]


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
        producedFile = fileInModuleDir(fileName)
        if os.path.exists(producedFile):
            os.remove(producedFile)
        result = executable(*args, **kw)
        self.assertTrue(
            os.path.exists(producedFile),
            "No file '{}' was produced.".format(
                producedFile))
        os.remove(producedFile)
        return result

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
