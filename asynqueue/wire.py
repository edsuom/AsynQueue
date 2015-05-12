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
L{WireWorker} and its support staff. B{Unsupported} and not yet
working. It's turned into a real mess. For most applications, you can
use L{process} instead.

You need to start a server that 
"""

import sys, os.path, tempfile, shutil, inspect

from zope.interface import implements
from twisted.internet import reactor, defer, endpoints
from twisted.internet.protocol import Factory
from twisted.python import reflect
from twisted.protocols import amp

from info import Info
from util import o2p, p2o
import errors, util, iteration
from interfaces import IWorker
from threads import ThreadLooper




class WireWorkerUniverse(object):
    """
    Subclass me in code that runs on the remote
    interpreter, and then call that 
    """

class WireWorker(object):
    """
    Runs tasks over the wire, via a TCP/IP connection and Twisted AMP.
    
    I implement an L{IWorker} that runs named tasks in a remote Python
    interpreter via Twisted's Asynchronous Messaging Protocol over
    TCP/IP. The task callable must be a method of a subclass of
    L{WireWorkerUniverse} that has been imported globally, as
    C{UNIVERSE}, into the same module as the one in which your
    instance of me is constructed. No pickled callables are sent over
    the wire, just strings defining the method name of that class
    instance.

    For most applications, see L{process.ProcessWorker} instead.

    You can also supply a I{series} keyword containing a list of one
    or more task series that I am qualified to handle.

    When running tasks via me, don't assume that you can just call
    blocking code because it's done remotely. The AMP server on the
    other end runs under Twisted, too, and the result of the call may
    be a deferred. If the call is a blocking one, set the I{thread}
    keyword C{True} for it and it will run via an instance of
    L{threads.ThreadLooper}.
    """
    implements(IWorker)
    pList = []
    tempDir = []
    cQualified = ['process', 'network']
    
    def __init__(self, host, port, series=[], reconnect=False, raw=False):
        self.ap = None
        self.tasks = []
        self.raw = raw
        self.iQualified = series
        # TODO: Implement reconnect option?
        self.reconnect = reconnect
        # Acquire lock until subordinate spawning and AMP connection
        self.dLock = util.DeferredLock()
        self._spawnAndConnect(sFile)
    
    def _spawnAndConnect(self, sFile=None):
        """
        Spawn a subordinate Python interpreter and connects to it via the
        AMP protocol and a unique UNIX socket address.
        """
        def ready(null):
            # Now connect to the AMP server via the UNIX socket
            dest = endpoints.UNIXClientEndpoint(reactor, sFile)
            return endpoints.connectProtocol(
                dest, amp.AMP()).addCallback(connected)

        def connected(ap):
            self.ap = ap
            # We finally have an AMP protocol object, ready for
            # callers to use, so release the lock
            self.dLock.release()
            self.pList.append(self.ap)

        if sFile is None:
            sFile = self._newSocketFile()
        if self.ap:
            # We already have a connection
            return defer.succeed(None)
        self.dLock.acquire()
        # Spawn the AMP server and "wait" for it to indicate it's OK
        args = [sys.executable, "-m", "asynqueue.wire", sFile]
        pp = ProcessProtocol(self._disconnected)
        self.pt = reactor.spawnProcess(pp, sys.executable, args)
        self.pid = self.pt.pid
        return pp.waitUntilReady().addCallback(ready)
        
    def _ditchClass(self):
        if hasattr(self, 'ap'):
            if self.ap in self.pList:
                self.pList.remove(self.ap)
            del self.ap

    def _disconnected(self, reason):
        self._ditchClass()
        if getattr(self, '_ignoreDisconnection', False):
            return
        possibleResignator = getattr(self, 'resignator', None)
        if callable(possibleResignator):
            possibleResignator()

    @defer.inlineCallbacks
    def _stopper(self):
        self._ditchClass()
        if hasattr(self, 'ap'):
            yield self.ap.callRemote(QuitRunning)
            yield self.ap.transport.loseConnection()
        if hasattr(self, 'pt'):
            yield self.pt.loseConnection()
        if hasattr(self, 'pid'):
            yield util.killProcess(self.pid)
            
    @defer.inlineCallbacks
    def assembleChunkedResult(self, ID):
        pickleString = ""
        while True:
            stuff = yield self.ap.callRemote(GetNext, ID=ID)
            chunk, isValid = stuff
            if isValid:
                pickleString += chunk
            else:
                break
        defer.returnValue(p2o(pickleString))

    @defer.inlineCallbacks
    def next(self, ID):
        """
        Do a next call of the iterator held by my subordinate, over the
        wire (socket) and in Twisted fashion.
        """
        yield self.dLock.acquire(vip=True)
        value, isValid = yield self.ap.callRemote(GetNext, ID=ID)
        self.dLock.release()
        value = result[0] if result[1] else Failure(StopIteration)
        defer.returnValue(value)

            
    # Implementation methods
    # -------------------------------------------------------------------------

    def setResignator(self, callableObject):
        self.resignator = callableObject
    
    @defer.inlineCallbacks
    def run(self, task):
        """
        Sends the task callable, args, kw to the process and returns a
        deferred to the eventual result.
        """
        def result(value):
            task.callback((status, value))
            self.tasks.remove(task)

        self.tasks.append(task)
        doNext = task.callTuple[2].pop('doNext', False)
        yield self.dLock.acquire(doNext)
        # Run the task on the subordinate Python interpreter
        #-----------------------------------------------------------
        kw = {}
        func = task.callTuple[0]
        for k, value in enumerate(task.callTuple):
            name = RunTask.arguments[k][0]
            kw[name] = value if isinstance(value, str) else o2p(value)
        if self.raw:
            kw.setdefault('raw', True)
        # The heart of the matter
        print "RUN-1", kw
        response = yield self.ap.callRemote(RunTask, **kw)
        print "RUN-2", response
        #-----------------------------------------------------------
        # At this point, we can permit another remote call to get
        # going for a separate task.
        self.dLock.release()
        # Process the response. No lock problems even if that
        # involves further remote calls, i.e., GetNext
        status = response['status']
        x = response['result']
        if status == 'i':
            # What we get from the subordinate is an ID to an iterator
            # it is holding onto, but we need to give the task an
            # iterationProducer that hooks up to it.
            pf = iteration.Prefetcherator(x)
            ok = yield pf.setup(self.next, x)
            if ok:
                dr = iteration.Deferator(pf)
                returnThis = iteration.IterationProducer(dr)
            else:
                # The subordinate returned an iterator, but it's not 
                # one I could prefetch from. Probably empty.
                returnThis = []
            result(returnThis)
        elif status == 'c':
            # Chunked result, which will take a while
            dResult = yield self.assembleChunkedResult(x)
            result(dResult)
        elif status == 'r':
            result(p2o(x))
        elif status == 'n':
            result(None)
        elif status == 'e':
            result(x)
        else:
            raise ValueError("Unknown status {}".format(status))

    def stop(self):
        self._ignoreDisconnection = True
        self.dLock.addStopper(self._stopper)
        return self.dLock.stop()

    def crash(self):
        if hasattr(self, 'ap') and self.ap in self.pList:
            self._stopper()
        return self.tasks


class RunTask(amp.Command):
    """
    Runs a task and returns the status and result.

    The I{methodName} is a string specifying the name of a method of a
    subclass of L{WireWorkerUniverse} that has been imported globally
    as C{UNIVERSE} into your AMP server. No callable will run that is
    not a regular, user-defined method of that object (no internal
    methods like C{__sizeof__}).

    But, see the I{Apache License}, section 8 ("Limitation of
    Liability"). There might be gaping security holes in this, and you
    should limit who you accept connections from in any event,
    preferably encrypting them.

    The args and kw are all pickled strings. (Be careful about
    allowing your methods to do arbitrary things with them!) The args
    and kw can be empty strings, indicating no args or kw.

    The response has the following status/result structure::

      'e': An exception was raised; the result is a pretty-printed
           traceback string.
  
      'n': Ran fine, the result was a C{None} object.
      
      'r': Ran fine, the result is the pickled return value of the call.
  
      'i': Ran fine, but the result is an iterable other than a standard
           Python one. The result is an ID string to use for your
           calls to C{GetNext}.
  
      'c': Ran fine, but the result is too big for a single return
           value. So you get an ID string for calls to C{GetNext.
    """
    arguments = [
        ('methodName', amp.String()),
        ('args', amp.String()),
        ('kw', amp.String()),
    ]
    response = [
        ('status', amp.String()),
        ('result', amp.String()),
    ]

class GetNext(amp.Command):
    """
    With a unique ID, get the next iteration of data from an iterator
    or a task result so big that it had to be chunked.

    The response has a 'value' string with the pickled iteration value
    or a chunk of the too-big task result, and an 'isValid' bool which
    is equivalent to a L{StopIteration}.
    """
    arguments = [
        ('ID', amp.String())
    ]
    response = [
        ('value', amp.String()),
        ('isValid', amp.Boolean()),
    ]

class QuitRunning(amp.Command):
    """
    Shutdown the reactor (after I'm done responding) and exit.
    """
    arguments = []
    response = []


class ProcessProtocol(object):
    """
    I am a simple protocol for a master Python interpreter to spawn
    and communicate with a subordinate Python. This protocol is used
    by the master (client).
    """
    def __init__(self, disconnectCallback=None):
        self.d = defer.Deferred()
        self.disconnectCallback = disconnectCallback

    def callback(self, text):
        if callable(self.disconnectCallback):
            self.disconnectCallback(text)
        
    def waitUntilReady(self):
        return self.d

    def makeConnection(self, process):
        pass

    def childDataReceived(self, childFD, data):
        data = data.strip()
        if childFD == 1 and data:
            self.d.callback(data)
        elif childFD == 2:
            self.callback("ERROR: {}".format(data))

    def childConnectionLost(self, childFD):
        self.callback("CONNECTION LOST!")

    def processExited(self, reason):
        self.callback(reason)

    def processEnded(self, reason):
        self.callback(reason)


class ChunkyString(object):
    """
    I iterate chunks of a big string, deleting my internal reference
    to it when done so it can be garbage collected even if I'm not.
    """
    chunkSize = 2**16 - 1

    def __init__(self, bigString):
        self.k0 = 0
        self.N = len(bigString)
        self.bigString = bigString
    
    def __iter__(self):
        return self

    def next(self):
        if not hasattr(self, 'bigString'):
            raise StopIteration
        k1 = min([self.k0 + self.chunkSize, self.N])
        thisChunk = self.bigString[self.k0:k1]
        if k1 == self.N:
            del self.bigString
        else:
            self.k0 = k1
        return thisChunk


class TaskUniverse(object):
    """
    I am the universe for all tasks running with a particular
    connection to my L{TaskServer}.

    """
    def __init__(self):
        self.iterators = {}
        self.deferators = {}
        self.info = Info()
        self.dt = util.DeferredTracker()

    def _saveIterator(x):
        ID = str(hash(x))
        self.iterators[ID] = x
        return ID
        
    @defer.inlineCallbacks
    def call(self, f, *args, **kw):
        """
        Run the f-args-kw combination, in the regular thread or in a
        thread running if I have one, returning a deferred to the
        status and result.
        """
        def oops(failureObj, ID=None):
            if ID:
                text = self.info.aboutFailure(failureObj, ID)
                self.info.forgetID(ID)
            else:
                text = self.info.aboutFailure(failureObj)
            return ('e', text)
        
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
            if isinstance(result, tuple) and result[0] == 'e':
                status, result = result
            elif result is None:
                # A None object
                status = 'n'
                result = ""
            elif not raw and iteration.Deferator.isIterator(result):
                status = 'i'
                result = self._saveIterator(result)
            else:
                status = 'r'
                result = o2p(result)
                if len(result) > ChunkyString.chunkSize:
                    # Too big to send as a single pickled string
                    status = 'c'
                    result = self._saveIterator(ChunkyString(result))
        # At this point, we have our result (blank string for C{None},
        # an ID for an iterator, or a pickled string
        response['status'] = status
        response['result'] = result
        defer.returnValue(response)
    
    @defer.inlineCallbacks
    def getNext(self, ID):
        """
        Gets the next item for the specified iterator, returning a
        deferred that fires with a response containing the pickled
        item and the isValid status indicating if the item is legit
        (False = StopIteration).
        """
        def oops(failureObj, ID):
            del self.iterators[ID]
            response['isValid'] = False
            if failureObj.type != StopIteration:
                response['value'] = self.info.setCall(
                    "getNext", [ID]).aboutFailure(failureObj)

        def bogusResponse():
            response['value'] = None
            response['isValid'] = False
            
        response = {'isValid': True}
        if ID in self.iterators:
            # Iterator
            if hasattr(self, 't'):
                # Get next iteration in a thread
                response['value'] = yield self.t.deferToThread(
                    self.iterators[ID].next).addErrback(oops, ID)
            else:
                # Get next iteration in main loop
                try:
                    response['value'] = self.iterators[ID].next()
                except StopIteration:
                    del self.iterators[ID]
                    bogusResponse()
        else:
            # No iterator, at least not anymore
            bogusResponse()
        defer.returnValue(response)
    
    def shutdown(self):
        if hasattr(self, 't'):
            d = self.t.stop().addCallback(lambda _: delattr(self, 't'))
            self.dt.put(d)
        return self.dt.deferToAll()


class TaskServer(amp.AMP):
    """
    The AMP server protocol for running tasks. The only security is
    the client's restricted access to whatever creates the protocol.
    """
    multiverse = {}
    shutdownDelay = 0.1

    def makeConnection(self, transport):
        self.info = util.Info()
        self.u = TaskUniverse()
        self.multiverse[id(transport)] = self.u
        return super(TaskServer, self).makeConnection(transport)
    
    def connectionLost(self, reason):
        super(TaskServer, self).connectionLost(reason)
        return self.quitRunning()

    #------------------------------------------------------------------
    def setNamespace(self, np):
        response = {}
        converter = p2o if '\n' in np.strip() else reflect.namedObject
        try:
            self.u.namespace = converter(np)
        except:
            response['status'] = self.info.setCall(
                "setNamespace", (np,), {}).aboutException()
        else:
            response['status'] = "OK"
        return response
    #------------------------------------------------------------------
    SetNamespace.responder(setNamespace)

    def _parseArg(self, fn):
        if '\n' in fn:
            # Looks like a pickle
            try:
                # Pickled callable?
                f = p2o(fn)
                if not callable(f):
                    f = None
            except:
                # Nope, and it wouldn't be anything else worth trying, either
                f = None
            return f
        f = getattr(getattr(self.u, 'namespace', None), fn, None)
        if callable(f):
            # Callable in set namespace 
            return f
        # Last resort: Callable in global namespace?
        try:
            f = reflect.namedObject(fn)
        except:
            # No, not that either.
            pass
        else:
            if callable(f):
                return f
    
    #------------------------------------------------------------------
    def runTask(self, fn, args, kw):
        """
        Responder for L{RunTask}
        """
        func = self._parseArg(fn)
        if func:
            args = p2o(args, [])
            kw = p2o(kw, {})
            return self.u.call(func, *args, **kw)
        text = self.info.setCall(fn, args, kw).aboutCall()
        return {
            'status': 'e',
            'result': "Couldn't identify callable for '{}'".format(text)}
    #------------------------------------------------------------------
    RunTask.responder(runTask)

    #------------------------------------------------------------------
    def getNext(self, ID):
        """
        Responder for L{GetNext}
        """
        return self.u.getNext(ID)
    #------------------------------------------------------------------
    GetNext.responder(getNext)

    #------------------------------------------------------------------
    def quitRunning(self):
        """
        Responder for L{QuitRunning}
        """
        def stopped(null):
            reactor.callLater(self.shutdownDelay, reactor.stop)
            return {}
        return defer.DeferredList([
            u.shutdown()
            for u in self.multiverse.itervalues()]).addBoth(stopped)
    #------------------------------------------------------------------
    QuitRunning.responder(quitRunning)


class TaskServerFactory(Factory):
    """
    I am a factory for a L{TaskServer} protocol, sending a handshaking
    code on stdout when a new AMP connection is made.
    """
    protocol = TaskServer
                 
    def startFactory(self):
        """
        Now that I'm ready to accept connections, sends the "OK"
        handshaking code to the ProcessWorker in my spawning Python
        interpreter via stdout.

        Might do something with hashing an authentication code here
        someday. Or not.
        """
        print "OK"
        sys.stdout.flush()
                 

def start(address):
    pf = TaskServerFactory()
    # Currently the security of UNIX domain sockets is the only
    # security we have!
    port = reactor.listenUNIX(address, pf)
    reactor.addSystemEventTrigger('before', 'shutdown', port.stopListening)
    reactor.run()


if __name__ == '__main__':
    address = sys.argv[-1]
    start(address)
    
