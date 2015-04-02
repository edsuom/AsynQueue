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
An implementor of the IWorker interface using (gasp! Twisted
heresy!) Python's stdlib multiprocessing.
"""
import multiprocessing as mp

from zope.interface import implements
from twisted.internet import defer

from interfaces import IWorker
import errors, util, iteration


class ProcessUniverse(object):
    """
    Each process for a L{ProcessWorker} lives in one of these.
    """
    def __init__(self):
        self.pfs = {}
        self.runner = util.CallRunner()
        
    def loop(self, connection):
        """
        Runs a loop in a dedicated process that waits for new tasks. The
        loop exits when a C{None} object is supplied as a task.
        """
        while True:
            # Wait here for the next call
            callSpec = connection.recv()
            if callSpec is None:
                # Termination call, no reply expected; just exit the
                # loop
                break
            elif isinstance(callSpec, int):
                # A getNext call, return the result of a getNext call
                # of the specified prefetcherator
                ID = callSpec
                if ID in self.pfs:
                    pf = self.pfs[ID]
                    value, isValid, moreLeft = pf.getNext()
                    if not moreLeft:
                        del self.pfs[ID]
                else:
                    value, isValid, moreLeft = None, False, False
                connection.send((value, isValid, moreLeft))
            else:
                # A task call
                status, result = self.runner(callSpec)
                if status == 'i':
                    # Due to the pipe between worker and process, we
                    # hold onto the prefetcherator here and just
                    # return an ID to it
                    ID = result.ID
                    self.pfs[ID] = result
                    result = ID
                connection.send((status, result))
        # Broken out of loop, ready for the process to end
        connection.close()


class ProcessWorker(object):
    """
    I implement an L{IWorker} that runs tasks in a dedicated worker
    process.

    You can also supply a series keyword containing a list of one or
    more task series that I am qualified to handle.

    All my tasks are run in a single instance of
    L{util.ThreadLoop}. Block away!
    """
    pollInterval = 0.01
    backOff = 1.04
    
    implements(IWorker)
    cQualified = ['process', 'local']

    def __init__(self, series=[], profiler=None):
        self.tasks = []
        self.iQualified = series
        self.profiler = profiler
        # Tools
        self.namespaces = {}
        self.dLock = util.DeferredLock()
        # Multiprocessing with (Gasp! Twisted heresy!) standard lib Python
        self.cMain, cProcess = mp.Pipe()
        pu = ProcessUniverse()
        self.process = mp.Process(target=pu.loop, args=(cProcess,))
        self.process.start()

    def _killProcess(self):
        self.cMain.close()
        self.process.terminate()

    @defer.inlineCallbacks
    def _getResponse(self):
        delay = self.pollInterval
        while True:
            if self.cMain.poll():
                defer.returnValue(self.cMain.recv())
                break
            # No response yet, check again after the poll interval,
            # which increases exponentially so that each incremental
            # delay is somewhat proportional to the amount of time
            # spent waiting thus far.
            yield iteration.deferToDelay(delay)
            delay *= self.backOff

    @defer.inlineCallbacks
    def _getNext(self, ID):
        yield self.dLock.acquire(vip=True)
        self.cMain.send(ID)
        response = yield self._getResponse()
        self.dLock.release()
        defer.returnValue(response)
            
    # Implementation methods
    # -------------------------------------------------------------------------
        
    def setResignator(self, callableObject):
        self.dLock.addStopper(callableObject)

    @defer.inlineCallbacks
    def run(self, task):
        """
        Sends the task callable and args, kw to the process (must all be
        picklable) and polls the interprocess connection for a result,
        with exponential backoff.
        """
        if task is None:
            # A termination task, do after pending tasks are done
            yield self.dLock.acquire()
            self.cMain.send(None)
            # Wait (a very short amount of time) for the process loop
            # to exit
            self.process.join()
        else:
            # A regular task
            self.tasks.append(task)
            yield self.dLock.acquire(task.priority <= -20)
            # Our turn!
            #------------------------------------------------------------------
            # Sanitize the task's callable
            #with task.sanitizeCall(raw=True) as ns:
            #    self.cMain.send(())
            self.cMain.send(task.callTuple)
            # "Wait" here (in Twisted-friendly fashion) for a response
            # from the process
            status, result = yield self._getResponse()
            if status == 'i':
                # What we get from the process is an ID to a
                # prefetcherator it is holding onto, but we
                # need to give the task an iterationProducer
                # that hooks up to the prefetcherator.
                ID = result
                dr = iteration.Deferator(ID, self._getNext, ID)
                result = iteration.IterationProducer(dr)
            if task in self.tasks:
                self.tasks.remove(task)
            task.callback((status, result))
        # Now ready for another task or shutdown
        self.dLock.release()
    
    def stop(self):
        """
        The returned deferred fires when the task loop has ended and its
        process terminated.
        """
        def terminationTaskDone(null):
            self._killProcess()
            return self.dLock.stop()            
        return self.run(None).addCallback(terminationTaskDone)

    def crash(self):
        self._killProcess()
        return self.tasks
