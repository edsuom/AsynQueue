# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
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
L{WireWorker} and its support staff. For most applications, you can
use L{process} instead.

You need to start another Python interpreter somewhere using
L{WireServer} and have L{WireWorker} connect to it via Twisted AMP. My
L{ServerManager} is just the thing for that.
"""

import sys, os, os.path, tempfile, shutil, inspect

from zope.interface import implementer
from twisted.python import reflect, failure
from twisted.internet import reactor, defer, endpoints
from twisted.protocols import amp
from twisted.internet.protocol import Factory
from twisted.application.service import Application
from twisted.application.internet import StreamServerEndpointService

from asynqueue.info import Info
from asynqueue.util import o2p, p2o
from asynqueue.interfaces import IWorker
from asynqueue.threads import ThreadLooper
from asynqueue import errors, util, iteration


DEFAULT_SOCKET = b"unix:/var/run/wire"
DEFAULT_WWU_FQN = "asynqueue.wire.WireWorkerUniverse"


class RunTask(amp.Command):
    """
    Runs a task and returns the status and result.

    The I{methodName} is a string specifying the name of a method of a
    subclass of L{WireWorkerUniverse}. No callable will run that is
    not a regular, user-defined method of that object (no internal
    methods like C{__sizeof__}).

    But, see the I{Apache License}, section 8 ("Limitation of
    Liability"). There might be gaping security holes in this, and you
    should limit who you accept connections from in any event,
    preferably encrypting them.

    The I{args} and I{kw} are all pickled strings. (Be careful about
    allowing your methods to do arbitrary things with them!) The args
    and kw can be empty strings, indicating no arguments or keywords.

    The response has the following status/result structure::

      'e': An exception was raised; the result is a pretty-printed
           traceback string.
  
      'n': Ran fine, the result was a C{None} object.
      
      'r': Ran fine, the result is the pickled return value of the call.
  
      'i': Ran fine, but the result is an iterable other than a standard
           Python one. The result is an ID string to use for your
           calls to C{GetNext}.
  
      'c': Ran fine, but the result is too big for a single return
           value. So you get an ID string for calls to C{GetNext}.
    """
    arguments = [
        (b'methodName', amp.String()),
        (b'args', amp.String()),
        (b'kw', amp.String()),
    ]
    response = [
        (b'status', amp.String()),
        (b'result', amp.String()),
    ]


class GetNext(amp.Command):
    """
    With a unique ID, gets the next iteration of data from an iterator
    or a task result so big that it had to be chunked.

    The response has a 'value' string with the pickled iteration value
    or a chunk of the too-big task result, and an 'isValid' bool which
    is equivalent to a L{StopIteration}.

    Strings don't need to be pickled, and if I{value} is an actual
    string, I{isRaw} will be set C{True}.
    """
    arguments = [
        (b'ID', amp.String())
    ]
    response = [
        (b'value', amp.String()),
        (b'isValid', amp.Boolean()),
        (b'isRaw', amp.Boolean()),
    ]


class WireWorkerUniverse(amp.CommandLocator):
    """
    Subclass me in code that runs on the remote interpreter, and then
    call the subclass methods via L{runTask}.

    Only methods you define in subclasses of this method, with names
    that don't start with an underscore, will be called.
    """
    @classmethod
    def check(cls, instance):
        for baseClass in inspect.getmro(instance.__class__):
            fqn = reflect.fullyQualifiedName(baseClass)
            if fqn == DEFAULT_WWU_FQN:
                return True
        raise TypeError(
            "You must provide a WireWorkerUniverse subclass instance")

    def __init__(self, wireServer=None):
        self.ws = wireServer
        super(WireWorkerUniverse, self).__init__()
    
    @RunTask.responder
    def runTask(self, methodName, args, kw):
        """
        This method is called to call the method specified by
        I{methodName} of my subclass running on the remote
        interpreter, with the supplied list I{args} of arguments and
        dict of keywords I{kw}, which may be empty.
        """
        methodName = p2o(methodName)
        if not hasattr(self, 'wr'):
            self.wr = WireRunner()
            self.info = Info()
        # The method must be a named attribute of my subclass
        # instance. No funny business with special '__foo__' type
        # methods, either.
        func = None if methodName.startswith('_') \
               else getattr(self, methodName, None)
        args = p2o(args, [])
        kw = p2o(kw, {})
        if callable(func):
            return self.wr.call(func, *args, **kw)
        # Wasn't a legit method call
        text = self.info.setCall(methodName, args, kw).aboutCall()
        return {
            b'status': b'e',
            b'result': "Couldn't run call '{}'".format(text)
        }

    @GetNext.responder
    def getNext(self, ID):
        """
        @see: L{GetNext}
        """
        return self.wr.getNext(ID)
        

@implementer(IWorker)
class WireWorker(object):
    """
    Runs tasks "over the wire," via Twisted AMP running on an
    C{endpoint} connection.
    
    I implement an L{IWorker} that runs named tasks in a remote Python
    interpreter via Twisted's Asynchronous Messaging Protocol over an
    endpoint that can be a UNIX socket, TCP/IP, SSL, etc. The task
    callable must be a method of a subclass of L{WireWorkerUniverse}
    that has been imported into the same module as the one in which
    your instance of me is constructed. No pickled callables are sent
    over the wire, just strings defining the method name of that class
    instance.

    For most applications, see L{process.ProcessWorker} instead.

    You can also supply a I{series} keyword containing a list of one
    or more task series that I am qualified to handle.

    When running tasks via me, don't assume that you can just call
    blocking code because it's done remotely. The AMP server on the
    other end runs under Twisted, too. (The result of the call may be
    a C{Deferred}, and that's fine.) If the call is a blocking one,
    set the I{thread} keyword C{True} for it and it will run via an
    instance of L{threads.ThreadLooper}.
    """
    pList = []
    tempDir = []
    cQualified = ['wire', 'remote']

    class AMP(amp.AMP):
        """
        Special disconnection-alerting AMP protocol. When my connection is
        made, I construct a C{Deferred} referenced as I{d_lcww}, which
        I will fire it if I get disconnected.
        """
        def connectionMade(self):
            self.d_lcww = defer.Deferred()
        def connectionLost(self, reason):
            if hasattr(self, 'd_lcww'):
                self.d_lcww.callback(None)
                del self.d_lcww
            return super(amp.AMP, self).connectionLost(reason)
    
    def __init__(
            self, wwu, description,
            series=[], raw=False, thread=False, N_concurrent=1):
        """
        Constructs me with a reference I{wwu} to a L{WireWorkerUniverse}
        and a client connection I{description}.

        Immediately connects to a L{WireServer} running on another
        Python interpreter via the AMP protocol.

        @keyword N_concurrent: The number of tasks I can have outstanding.
        """
        def connected(ap):
            self.ap = ap
            self.dLock.release()
        
        WireWorkerUniverse.check(wwu)
        self.tasks = []
        self.raw = raw
        self.thread = thread
        self.iQualified = series
        # Lock that is acquired until AMP connection made
        self.dLock = util.DeferredLock(allowZombies=True)
        self.dLock.addStopper(self.stopper)
        self.dLock.acquire()
        # Limit tasks outstanding
        self.ds = defer.DeferredSemaphore(N_concurrent)
        # Make the connection
        dest = endpoints.clientFromString(reactor, description)
        endpoints.connectProtocol(
            dest, self.AMP(locator=wwu)).addCallback(connected)

    def stopper(self):
        if hasattr(self, 'ap'):
            return self.ap.transport.loseConnection()

    def _handleNext(self, ID):
        def gotResponse(response):
            if response['isValid']:
                value = response['value']
                if response['isRaw']:
                    return True, value
                return True, p2o(value)
            return False, None
        return self.ap.callRemote(GetNext, ID=ID).addCallback(gotResponse)
    
    @defer.inlineCallbacks
    def assembleChunkedResult(self, ID):
        pickleString = ""
        while True:
            isValid, value = yield self._handleNext(ID)
            if isValid:
                pickleString += value
            else:
                break
        defer.returnValue(p2o(pickleString))

    def do_next(self, ID):
        """
        Do a next call of the iterator held by my subordinate, over the
        wire (socket) and in Twisted fashion.
        
        This is done with highest priority because something is
        waiting for possibly lots of calls of this to complete.
        """
        def ready(null):
            return self._handleNext(ID).addCallback(gotNext)
        def gotNext(stuff):
            isValid, value = stuff
            self.dLock.release()
            if not isValid:
                raise StopIteration
            return value
        return self.dLock.acquire(vip=True).addCallback(ready)

    def resign(self, *args):
        if hasattr(self, 'resignator'):
            self.resignator()
            del self.resignator
        
    # Implementation methods
    # -------------------------------------------------------------------------
    
    def setResignator(self, callableObject):
        """
        I resign if my underlying AMP connection is lost.
        """
        def gotLock(lock):
            self.ap.d_lcww.addCallback(self.resign)
            lock.release()
        self.resignator = callableObject
        self.dLock.acquire().addCallback(gotLock)
    
    @defer.inlineCallbacks
    def run(self, task):
        """
        Sends the task callable, args, kw to the process and returns a
        deferred to the eventual result.
        """
        def result(value):
            self.tasks.remove(task)
            task.callback((status, value))

        self.tasks.append(task)
        doNext = task.callTuple[2].pop('doNext', False)
        yield self.dLock.acquire(doNext)
        self.dLock.release()
        yield self.ds.acquire()
        # Run the task via AMP, but only if it's connected
        #-----------------------------------------------------------
        if not self.ap.transport.connected:
            response = {'status':b'd', 'result':None}
        else:
            kw = {}
            consumer = task.callTuple[2].pop('consumer', None)
            for k, value in enumerate(task.callTuple):
                name = RunTask.arguments[k][0].decode()
                kw[name] = o2p(value)
            if self.raw:
                kw.setdefault('raw', True)
            if self.thread:
                kw.setdefault('thread', True)
            # The heart of the matter
            try:
                response = yield self.ap.callRemote(RunTask, **kw)
            except:
                response = {'status':b'd', 'result':None}
        #-----------------------------------------------------------
        # One less task running now
        self.ds.release()
        # Process the response. No lock problems even if that
        # involves further remote calls, i.e., GetNext
        status = response['status']
        x = response['result']
        if status == b'i':
            # What we get from the subordinate is an ID to an iterator
            # it is holding onto, but we need to give the task an
            # iterationProducer that hooks up to it.
            pf = iteration.Prefetcherator(x)
            ok = yield pf.setup(self.do_next, x)
            if ok:
                returnThis = iteration.Deferator(pf)
                if consumer:
                    returnThis = iteration.IterationProducer(
                        returnThis, consumer)
            else:
                # The subordinate returned an iterator, but it's not 
                # one I could prefetch from. Probably empty.
                returnThis = []
            result(returnThis)
        elif status == b'c':
            # Chunked result, which will take a while
            dResult = yield self.assembleChunkedResult(x)
            result(dResult)
        elif status == b'r':
            result(p2o(x))
        elif status == b'n':
            result(None)
        elif status in (b'e', b'd'):
            result(x)
            self.resign()
        else:
            raise ValueError("Unknown status {}".format(status))

    def stop(self):
        if getattr(self, '_stopped', False):
            return
        self._stopped = True
        return self.dLock.stop()

    def crash(self):
        self.stopper()
        return self.tasks


class ChunkyString(object):
    """
    I iterate chunks of a big string, deleting my internal reference
    to it when done so it can be garbage collected even if I'm not.
    """
    chunkSize = 2**15

    def __init__(self, bigString):
        self.k0 = 0
        self.N = len(bigString)
        self.bigString = bigString
    
    def __iter__(self):
        return self

    def __next__(self):
        if not hasattr(self, 'bigString'):
            raise StopIteration
        k1 = min([self.k0 + self.chunkSize, self.N])
        thisChunk = self.bigString[self.k0:k1]
        if k1 == self.N:
            del self.bigString
        else:
            self.k0 = k1
        return thisChunk


class WireRunner(object):
    """
    An instance of me is constructed by a L{WireWorkerUniverse} on the
    server end of the AMP connection to run all tasks for its
    L{WireServer}.
    """
    def __init__(self):
        self.iterators = {}
        self.deferators = {}
        self.info = Info()
        self.dt = util.DeferredTracker()

    def shutdown(self):
        if hasattr(self, 't'):
            d = self.t.stop().addCallback(lambda _: delattr(self, 't'))
            self.dt.put(d)
        return self.dt.deferToAll()
    
    def _saveIterator(self, x):
        ID = str(hash(x)).encode()
        self.iterators[ID] = x
        return ID
    
    @defer.inlineCallbacks
    def _call(self, f, *args, **kw):
        def oops(failureObj, ID=None):
            if ID:
                text = self.info.aboutFailure(failureObj, ID)
                self.info.forgetID(ID)
            else:
                text = self.info.aboutFailure(failureObj)
            return (b'e', text)

        response = {}
        raw = kw.pop('raw', False)
        if kw.pop('thread', False):
            if not hasattr(self, 't'):
                self.t = ThreadLooper(rawIterators=True)
            # No errback needed because L{util.CallRunner} returns an
            # 'e' status for errors
            status, result = yield self.t.call(f, *args, **kw)
        else:
            # The info object saves the call
            self.info.setCall(f, args, kw)
            ID = self.info.ID
            result = yield defer.maybeDeferred(
                f, *args, **kw).addErrback(oops, ID)
            self.info.forgetID(ID)
            if isinstance(result, tuple) and result[0] == b'e':
                status, result = result
            elif result is None:
                # A None object
                status = b'n'
                result = b""
            elif not raw and iteration.Deferator.isIterator(result):
                status = b'i'
                result = self._saveIterator(result)
            else:
                status = b'r'
                result = o2p(result)
                if len(result) > ChunkyString.chunkSize:
                    # Too big to send as a single pickled string
                    status = b'c'
                    result = self._saveIterator(ChunkyString(result))
        # At this point, we have our result (blank string for C{None},
        # an ID for an iterator, or a pickled string
        response['status'] = status
        response['result'] = result
        defer.returnValue(response)

    def call(self, f, *args, **kw):
        """
        Run the f-args-kw combination, in the regular thread or in a
        thread running if I have one.

        @return: A C{Deferred} to the status and result.
        """
        d = self._call(f, *args, **kw)
        self.dt.put(d)
        return d
        
    @defer.inlineCallbacks
    def _getNext(self, ID):
        def oops(failureObj, ID):
            del self.iterators[ID]
            response['isValid'] = False
            if failureObj.type == StopIteration:
                response['value'] = ""
            else:
                response['value'] = self.info.setCall(
                    "getNext", [ID]).aboutFailure(failureObj)
        def bogusResponse():
            response['value'] = b""
            response['isValid'] = False
        def handleValue(value):
            if isinstance(value, bytes):
                response['isRaw'] = True
                response['value'] = value
            else:
                response['value'] = o2p(value)
            
        response = {'isValid':True, 'isRaw':False}
        if ID in self.iterators:
            # Iterator
            if hasattr(self, 't'):
                # Get next iteration in a thread
                value = yield self.t.deferToThread(
                    self.iterators[ID].__next__).addErrback(oops, ID)
                if response['isValid']:
                    handleValue(value)
            else:
                # Get next iteration in main loop. No blocking!
                try:
                    value = self.iterators[ID].__next__()
                    handleValue(value)
                except StopIteration:
                    del self.iterators[ID]
                    bogusResponse()
        else:
            # No iterator, at least not anymore
            bogusResponse()
        defer.returnValue(response)

    def getNext(self, ID):
        """
        Gets the next item for the iterator specified by I{ID}.

        Returns a C{Deferred} that fires with a response containing
        the pickled item and the I{isValid} status indicating if the
        item is legit (C{False} = L{StopIteration}).
        """
        d = self._getNext(ID)
        self.dt.put(d)
        return d


class WireServer(object):
    """
    An AMP server for the remote end of a L{WireWorker}.
    
    Construct me with an endpoint description string and either an
    instance or the fully qualified name of a L{WireWorkerUniverse}
    subclass.

    @ivar service: A C{StreamServerEndpointService} from
        C{twisted.application.internet} that you can include in the
        C{application} of a C{.tac} file, thus accepting connections
        to run tasks.
    """
    triggerID = None
    
    def __init__(self, description, wwu):
        if isinstance(wwu, str):
            klass = reflect.namedObject(wwu)
            wwu = klass(self)
        WireWorkerUniverse.check(wwu)
        self.factory = Factory()
        self.factory.protocol = lambda: amp.AMP(locator=wwu)
        endpoint = endpoints.serverFromString(reactor, description)
        self.service = StreamServerEndpointService(endpoint, self.factory)

    def start(self):
        self.service.startService()
        self.triggerID = reactor.addSystemEventTrigger(
            'before', 'shutdown', self.stop)
        
    def stop(self):
        if self.triggerID is None:
            return defer.succeed(None)
        self.triggerID = None
        return self.service.stopService()

        
class ServerManager(object):
    """
    I spawn one or more new Python interpreters that run a
    L{WireServer} on the local machine.
    """
    def __init__(self, wwuFQN=None):
        self.processInfo = {}
        self.wwuFQN = DEFAULT_WWU_FQN if wwuFQN is None else wwuFQN
        self.triggerID = reactor.addSystemEventTrigger(
            'before', 'shutdown', self.shutdown)

    @defer.inlineCallbacks
    def shutdown(self):
        if self.triggerID is not None:
            reactor.removeSystemEventTrigger(self.triggerID)
            self.triggerID = None
            yield self.done()
            if hasattr(self, 'tempDir'):
                try: shutil.rmtree(self.tempDir)
                except: pass
                del self.tempDir

    @defer.inlineCallbacks
    def spawn(self, description, stdio=False, niceness=0):
        """
        Spawns a subordinate Python interpreter.

        B{TODO:} Implement (somehow) I{niceness} keyword to accept an
        integer UNIX nice level for the new interpreter process.

        @param description: A server description string of the form
            used by Twisted's C{endpoints.serverFromString}.

        @return: A C{Deferred} that fires when the new process is
            running. If in I{stdio} mode, it fires with a
            L{util.ProcessProtocol} (which includes a I{pid}
            attribute) for the new process. Otherwise, fires with the
            integer I{pid} of the new process.
        """
        result = None
        # Spawn the AMP server and "wait" for it to indicate it's OK
        args = [
            sys.executable,
            "-m", "asynqueue.wire", description, self.wwuFQN]
        if stdio:
            pp = util.ProcessProtocol(self.done)
            pt = reactor.spawnProcess(pp, sys.executable, args)
            response = yield pp.d
            self.processInfo[pt.pid] = {'pt':pt}
            if b"AsynQueue WireServer listening" in response:
                result = pp
            else: self.done(pt.pid)
        else:
            checker = iteration.Delay()
            pid = os.spawnl(os.P_NOWAIT, sys.executable, *args)
            self.processInfo[pid] = None
            socketPath = description.split(':')[1]
            yield checker.untilEvent(os.path.exists, socketPath)
            result = pid
        defer.returnValue(result)
    
    def newSocket(self):
        """
        Assigns a unique name to a socket file in a temporary directory
        common to all processes spawned by me, which will be removed
        with all socket files after reactor shutdown. Doesn't actually
        create the socket file; the server does that.

        @return: An endpoint description using the new socket filename.
        """
        # The process name
        pName = "worker-{:03d}".format(len(self.processInfo))
        # A unique temp directory for all instances' socket files
        if not hasattr(self, 'tempDir'):
            self.tempDir = tempfile.mkdtemp()
        socketFile = os.path.join(self.tempDir, "{}.sock".format(pName))
        return 'unix:' + socketFile
    
    @defer.inlineCallbacks
    def done(self, pid=None):
        if pid is None:
            for pid in list(self.processInfo.keys()):
                yield self.done(pid)
        elif pid in self.processInfo:
            thisInfo = self.processInfo[pid]
            if thisInfo is not None:
                if 'ap' in thisInfo:
                    yield thisInfo['ap'].transport.loseConnection()
                if 'pt' in thisInfo:
                    yield thisInfo['pt'].loseConnection()
            yield util.killProcess(pid)
            del self.processInfo[pid]

        
def runServer(description, wwuFQN):
    """
    Runs a L{WireServer}, listening at the specified endpoint
    I{description} without bothering with an C{application}.

    You must specify the package.module.class fully qualified name of
    a L{WireWorkerUniverse} subclass with I{wwu}.
    """
    def running():
        ws.start()
        print("AsynQueue WireServer listening at {}".format(description))
        sys.stdout.flush()
    
    ws = WireServer(description, wwuFQN)
    reactor.callWhenRunning(running)
    reactor.run()

        
if __name__ == '__main__':
    runServer(*sys.argv[1:])


