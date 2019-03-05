## AsynQueue
*Asynchronous task queueing with Twisted: threaded, multicore, and remote.*

* [API Docs](http://edsuom.com/AsynQueue/asynqueue.html)
* [PyPI Page](https://pypi.python.org/pypi/AsynQueue/)
* [Project Page](http://edsuom.com/AsynQueue.html) at **edsuom.com**
* *See also* [sAsync](http://edsuom.com/sAsync.html)


**AsynQueue** provides asynchronous task queueing based on the
[Twisted](http://twistedmatrix.com) framework, with task
prioritization and a powerful worker interface. Worker implementations
are included for running tasks asynchronously in the main thread, in
separate threads, and in separate Python interpreters
(multiprocessing).

Includes deferred iteration capability: Calling a task that returns an
iterator can return a
[Deferator](http://edsuom.com/AsynQueue/asynqueue.iteration.Deferator.html)
instead, which does the iteration in a Twisted-friendly fashion, even
over a network connection. You can also supply an object conforming to
Twisted's IConsumer interface and iterations will be fed to it as they
become available.

The *util* module contains a
[DeferredTracker](http://edsuom.com/AsynQueue/asynqueue.util.DeferredTracker.html)
object that makes the import worthwhile all on its own. You can use
its **put** method to track Twisted *Deferred* objects without inserting
anything into their callback chains. Then you can wait in non-blocking
Twisted fashion for all, any, or some of the tracked deferreds to fire
(again, without getting tangled up with any of their callbacks) using
the tracker's **deferToAll**, **deferToAny**, and **deferUntilFewer**
methods.

There's a detailed usage example below. Also, see the one provided in
the example package "mcmandelbrot" that accompanies this README file
and installs with asynqueue. There is a console entry point for
`mcm`. Give it a try and see what asynchronous multiprocessing can do.


### Multicore Made Easy

Here is a simplified example of how I use it to get a few CPU cores
parsing logfiles in the [logalyzer](http://edsuom.com/logalyzer.html)
package:

```python
class Reader:
    # Maximum number of logfiles to process concurrently
    N = 6

    # <Stuff...>

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
```

### Process Queueing

The
[Reader](http://edsuom.com/logalyzer/logalyzer.logread.Reader.html)
object has a
[ProcessReader](http://edsuom.com/logalyzer/logalyzer.logread.ProcessReader.html)
object, referenced by its `self.pr` attribute. The process reader is
passed to subordinate Python processes for doing the logfile parsing.

In fact, each call to the reader object's task queue,
[base.TaskQueue.call](http://edsuom.com/AsynQueue/asynqueue.base.TaskQueue.html#call)
via the
[process.ProcessQueue](http://edsuom.com/AsynQueue/asynqueue.process.ProcessQueue.html)
subclass instance, passes along a reference to `self.pr` as a
callable. But that's not a problem, even over the interprocess
pipe. Python's built-in `multiprocessing` module pickles the
reference very efficiently, and almost no CPU time is spent doing so.

Everything done by each subordinate Python process is contained in the
following two methods of its copy of the
[process.ProcessUniverse](http://edsuom.com/AsynQueue/asynqueue.process.ProcessUniverse.html)
object:

```python
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
```

### Bridging the Blocking Gap

Yes, the process blocks when it waits for the next call with
`connection.recv`. No a problem in this case, because it's not running
Twisted; the subordinate Python interpreter's whole purpose in life is
to run tasks sent to it via the task queue. And on the main
Twisted-running interpreter, here's what
[process.ProcessWorker](http://edsuom.com/AsynQueue/asynqueue.workers.ProcessWorker.html)
does. Note the magic that happens in the line with `yield
self.delay.untilEvent(self.cMain.poll)`:

```python
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
```

The
[iteration.Delay](http://edsuom.com/AsynQueue/asynqueue.iteration.Delay.html)
object has this very cool capability of providing a Deferred that
fires after an event happens. It checks whatever no-argument callable
you provide to see if the event has happened yet, and fires the
Deferred if so. If not, it waits a while and checks again, with
exponential back off to keep the interval between checks approximately
proportionate to the amount of time that's passed. It's efficient and
works very well.


### Iterations, Twisted-Style

The main Reader object running on the main Python interpreter also has
a
[RecordKeeper](http://edsuom.com/logalyzer/logalyzer.records.RecordKeeper.html)
object, referenced by `self.rk`, that can provide implementors of
`twisted.internet.interfaces.IConsumer`. Those consumer objects
receive the iterations that are produced by
`iteration.IterationProducer` instances, iterating asynchronously
"over the wire" (actually, over the interprocess connection pipe).


### License

Copyright (C) 2006-2007, 2015, 2018-19 by Edwin A. Suominen

See [edsuom.com](http://edsuom.com) for API documentation as well as
information about Ed's background and other projects, software and
otherwise.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  <http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an "AS
IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language
governing permissions and limitations under the License.
