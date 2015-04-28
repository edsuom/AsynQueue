"""
Priority queueing of tasks to one or more threaded or asynchronous workers.

B{AsynQueue} provides asynchronous task queueing based on the Twisted
framework, with task prioritization and a powerful worker
interface. Worker implementations are included for running tasks
asynchronously in the main thread, in separate threads, and in
separate Python interpreters (multiprocessing).


Example
=======

Here is a simplified example of how I use it to get a few CPU cores
parsing logfiles::

  class Reader:
      # Maximum number of logfiles to process concurrently
      N = 6

      <stuff...>

      @defer.inlineCallbacks
      def run(self):
          def dispatch(fileName):
              filePath = self.pathInDir(fileName)
              # Get a ProcessConsumer for this file
              consumer = self.rk.consumerFactory(fileName)
              self.consumers.append(consumer)
              # Call the ProcessReader on one of my subordinate
              # processes to have it feed the consumer with
              # misbehaving IP addresses and filtered records
              return self.pq.call(
                  self.pr, filePath, consumer=consumer).addCallback(done)

          def done(consumer):
              self.consumers.remove(consumer)
  
          dList = []
          self.lock.acquire()
          self.pq = asynqueue.ProcessQueue(3)
          # We have at most two files being parsed concurrently for each
          # worker servicing my process queue
          ds = defer.DeferredSemaphore(min([self.N, 2*len(self.pq)]))
          # "Wait" for everything to start up and get a list of
          # known-bad IP addresses
          ipList = yield self.rk.startup()
          
          # Warn workers to ignore records from the bad IPs
          yield self.pq.update(self.pr.ignoreIPs, ipList)
  
          # Dispatch files as permitted by the semaphore
          for fileName in self.fileNames:
              if not self.isRunning():
                  break
              # "Wait" for the number of concurrent parsings to fall
              # back to the limit
              yield ds.acquire()
              # If not running, break out of the loop
              if not self.isRunning():
                  break
              # References to the deferreds from dispatch calls are
              # stored in the process queue, and we wait for their
              # results.
              d = dispatch(fileName)
              d.addCallback(lambda _: ds.release())
              dList.append(d)
          yield defer.DeferredList(dList)
          ipList = self.rk.getNewIPs()
          defer.returnValue(ipList)
          # Can now shut down, regularly or due to interruption
          self.lock.release()


Process Queueing
================

That C{Reader} object has a C{ProcessReader} object, referenced by its
I{self.pr} attribute. The process reader is passed to subordinate
Python processes for doing the logfile parsing.

In fact, each call to the reader object's task queue,
L{base.TaskQueue.call} via the L{process.ProcessQueue} subclass
instance, passes along a reference to I{self.pr} as a callable. But
that's not a problem, even over the interprocess pipe. Python's
built-in C{multiprocessing} module pickles the reference very
efficiently, and almost no CPU time is spent doing so.

Everything done by each subordinate Python process is contained in the
following two methods of its copy of the L{process.ProcessUniverse}
object::

    def next(self, ID):
        if ID in self.iterators:
            try:
                value = self.iterators[ID].next()
            except StopIteration:
                del self.iterators[ID]
                return None, False
            return value, True
        return None, False

    def loop(self, connection):
        while True:
            # Wait here for the next call
            callSpec = connection.recv()
            if callSpec is None:
                # Termination call, no reply expected; just exit the
                # loop
                break
            elif isinstance(callSpec, str):
                # A next-iteration call
                connection.send(self.next(callSpec))
            else:
                # A task call
                status, result = self.runner(callSpec)
                if status == 'i':
                    # Due to the pipe between worker and process, we
                    # hold onto the iterator here and just
                    # return an ID to it
                    ID = str(hash(result))
                    self.iterators[ID] = result
                    result = ID
                connection.send((status, result))
        # Broken out of loop, ready for the process to end
        connection.close()

Yes, the process blocks when it waits for the next call with
C{connection.recv}. So what? It's not running Twisted; the subordinate
Python interpreter's whole purpose in life is to run tasks sent to it
via the task queue. And on the main Twisted-running interpreter,
here's what L{process.ProcessWorker} does. Note the magic that happens
in the line with C{yield self.delay.untilEvent(self.cMain.poll)}::

    @defer.inlineCallbacks
    def run(self, task):
        if task is None:
            # A termination task, do after pending tasks are done
            yield self.dLock.acquire()
            self.cMain.send(None)
            # Wait (a very short amount of time) for the process loop
            # to exit
            self.process.join()
            self.dLock.release()
        else:
            # A regular task
            self.tasks.append(task)
            yield self.dLock.acquire(task.priority <= -20)
            # Our turn!
            #------------------------------------------------------------------
            consumer = task.callTuple[2].pop('consumer', None)
            self.cMain.send(task.callTuple)
            # "Wait" here (in Twisted-friendly fashion) for a response
            # from the process
            yield self.delay.untilEvent(self.cMain.poll)
            status, result = self.cMain.recv()
            self.dLock.release()
            if status == 'i':
                # What we get from the process is an ID to an iterator
                # it is holding onto, but we need to hook up to it
                # with a Prefetcherator and then make a Deferator,
                # which we will either return to the caller or couple
                # to a consumer provided by the caller.
                ID = result
                pf = iteration.Prefetcherator(ID)
                ok = yield pf.setup(self.next, ID)
                if ok:
                    result = iteration.Deferator(pf)
                    if consumer:
                        result = iteration.IterationProducer(result, consumer)
                else:
                    # The process returned an iterator, but it's not 
                    # one I could prefetch from. Probably empty.
                    result = []
            if task in self.tasks:
                self.tasks.remove(task)
            task.callback((status, result))

The L{iteration.Delay} object has this very cool capability of
providing a C{Deferred} that fires after an event happens. It checks
whatever no-argument callable you provide to see if the event has
happened yet, and fires the C{Deferred} if so. If not, it waits a
while and checks again, with exponential back off to keep the interval
between checks approximately proportionate to the amount of time
that's passed. It's efficient and works very well.


Iterations, Twisted-Style
=========================

The main C{Reader} object running on the main Python interpreter also
has a C{RecordKeeper} object, reference by I{self.rk}, that can
provide implementors of
C{twisted.internet.interfaces.IConsumer}. Those consumer objects
receive the iterations that are produced by
L{iteration.IterationProducer} instances, iterating asynchronously
"over the wire" (actually, over the interprocess connection pipe).


License
=======

Copyright (C) 2006-2007, 2015 by Edwin A. Suominen,
U{http://edsuom.com/}. This program is free software: you can
redistribute it and/or modify it under the terms of the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version. This
program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
for more details. You should have received a copy of the GNU General
Public License along with this program.  If not, see
U{http://www.gnu.org/licenses/}.

@author: Edwin A. Suominen
"""

from workers import *
from base import TaskQueue
from threads import ThreadQueue
from process import ProcessQueue
from info import showResult, Info
from util import DeferredTracker, DeferredLock
