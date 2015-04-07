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
Unit tests for asynqueue.workers
"""

import time, random
from twisted.internet import defer

from util import TestStuff
import base, tasks, workers
from testbase import deferToDelay, TestCase, IterationConsumer


class TestAsyncWorker(TestCase):
    verbose = False
    
    def setUp(self):
        self.worker = workers.AsyncWorker()
        self.queue = base.TaskQueue()
        self.queue.attachWorker(self.worker)

    def tearDown(self):
        return self.queue.shutdown()

    def _twistyTask(self, x):
        delay = random.uniform(0.1, 0.5)
        self.msg("Running {:f} sec. async task", delay)
        return deferToDelay(delay).addCallback(lambda _: 2*x)
        
    def test_call(self):
        d = self.queue.call(self._twistyTask, 2)
        d.addCallback(self.assertEqual, 4)
        return d

    def test_multipleTasks(self):
        N = 5
        expected = [2*x for x in xrange(N)]
        for k in self.multiplerator(N, expected):
            self.d = self.queue.call(self._twistyTask, k)
        return self.dm

    def test_multipleCalls(self):
        N = 5
        expected = [('r', 2*x) for x in xrange(N)]
        worker = workers.AsyncWorker()
        for k in self.multiplerator(N, expected):
            task = tasks.Task(self._twistyTask, (k,), {}, 0, None)
            self.d = task.d
            worker.run(task)
        return self.dm.addCallback(lambda _: worker.stop())

    @defer.inlineCallbacks
    def test_iteration(self):
        N1, N2 = 50, 100
        stuff = TestStuff()
        stuff.setStuff(N1, N2)
        dr = yield self.queue.call(stuff.stufferator)
        chunks = []
        for d in dr:
            chunk = yield d
            self.assertEqual(len(chunk), N1)
            chunks.append(chunk)
        self.assertEqual(len(chunks), N2)

    @defer.inlineCallbacks
    def test_iterationProducer(self):
        N1, N2 = 50, 100
        stuff = TestStuff()
        stuff.setStuff(N1, N2)
        consumer = IterationConsumer(self.verbose)
        yield self.queue.call(stuff.stufferator, consumer=consumer)
        for chunk in consumer.data:
            self.assertEqual(len(chunk), N1)
        self.assertEqual(len(consumer.data), N2)
