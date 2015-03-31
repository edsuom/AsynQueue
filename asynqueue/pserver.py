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
This module is imported by a subordinate Python process to service
a ProcessWorker.
"""

import sys, traceback

from twisted.internet import reactor, defer
from twisted.internet.protocol import Factory
from twisted.python import reflect
from twisted.protocols import amp

import errors, util, iteration
from util import o2p, p2o


class SetNamespace(amp.Command):
    """
    Sets the namespace for named callables used in L{RunTask}.

    Supply either a pickled object with one or more methods you want
    to use or a fully named module-global callable that can be
    imported via L{twisted.python.reflect.namedObject}.
    """
    arguments = [
        ('np', amp.String())
    ]
    response = [
        ('status', amp.String())
    ]


class RunTask(amp.Command):
    """
    Runs a task and returns the status and result.

    The callable is either a pickled callable object or the name of a
    callable, either in a previously set namespace or the global
    namespace (with any intervening modules imported automatically).

    The args and kw are all pickled strings. The args and kw can be
    empty strings, indicating no args or kw.

    The response has the following status/result structure:

    'e': An exception was raised; the result is a pretty-printed
         traceback string.

    'n': Ran fine, the result was a C{None} object.
    
    'r': Ran fine, the result is the pickled return value of the call.

    'i': Ran fine, but the result is an iterable other than a standard
         Python one. The result is an ID string to use for your
         calls to C{GetMore}.

    'c': Ran fine, but the result is too big for a single return
         value. So you get an ID string for calls to C{GetMore}.
    """
    arguments = [
        ('fn', amp.String()),
        ('args', amp.String()),
        ('kw', amp.String()),
    ]
    response = [
        ('status', amp.String()),
        ('result', amp.String()),
    ]


class GetMore(amp.Command):
    """
    With a unique ID, get the next iteration of data from an iterator
    or a task result so big that it had to be chunked.

    The response has a 'value' string with the pickled iteration value
    or a chunk of the too-big task result, an 'isValid' bool (which
    should be True for all cases except an iterable that's empty from
    the start), and a 'moreLeft' Bool indicating if you should call me
    again with this ID for another iteration or chunk.
    """
    arguments = [
        ('ID', amp.String())
    ]
    response = [
        ('value', amp.String()),
        ('isValid', amp.Boolean()),
        ('moreLeft', amp.Boolean()),
    ]


class QuitRunning(amp.Command):
    """
    Shutdown the reactor (after I'm done responding) and exit.
    """
    arguments = []
    response = []


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
        self.pfs = {}
        self.info = util.Info()
        self.dt = util.DeferredTracker()

    def pf(self, ID):
        """
        Returns the Prefetcherator of record for a particular ID,
        constructing a new one if necessary.
        """
        if ID not in self.pfs:
            self.pfs[ID] = iteration.Prefetcherator(ID)
        return self.pfs[ID]
    
    def _handleIterator(self, result, response):
        """
        Handles a result that is an iterator.

        Prefetcherators use hardly any memory, so we keep one around
        for each f-args-kw combo resulting in calls to this method.
        """
        # Try the iterator on for size
        ID = response['ID']
        pf = self.pf(ID)
        if pf.setup(result):
            response['status'] = 'i'
            response['result'] = ID
            return
        # Aw, can't do the iteration, try an iterator-as-list fallback
        try:
            pickledResult = list(result)
        except:
            response['status'] = 'e'
            text = "Failed to iterate for task {}".format(
                self.info.aboutCall(ID))
            response['result'] = text
        else:
            self.pickledResult(pickledResult)

    def _handlePickle(self, pickledResult, response):
        """
        Handles a regular result that's been pickled for transmission.
        """
        if len(pickledResult) > ChunkyString.chunkSize:
            # Too big to send as a single pickled result
            response['status'] = 'c'
            ID = response['ID']
            self.pf(ID).setup(ChunkyString(pickledResult))
            response['result'] = ID
        else:
            # Small enough to just send
            response['status'] = 'r'
            response['result'] = pickledResult
    
    def call(self, f, *args, **kw):
        """
        """
        def ran(result):
            if result is None:
                # Result is a None object
                response['status'] = 'n'
                response['result'] = ""
            elif isinstance(result, iteration.Deferator):
                # Result is a Deferator, just tell my prefetcherator
                # to use its getNext function.
                df, dargs, dkw = result.callTuple
                self.pf(ID).setup(df, *dargs, **dkw)
            elif iteration.Deferator.isIterator(result):
                # It's a Deferator-able iterator
                self._handleIterator(result, response)
            else:
                # It's a regular result
                self._handlePickle(o2p(result), response)

        def oops(failureObj):
            response['status'] = 'e'
            response['result'] = self.info.aboutFailure(
                failureObj, response['ID'])

        def done(null):
            self.info.forgetID(response.pop('ID'))
            return response
            
        response = {'ID': str(self.info.setCall(f, args, kw).getID())}
        if kw.pop('thread', False):
            if not hasattr(self, 't'):
                self.t = util.ThreadLooper()
            d = self.t.deferToThread(f, *args, **kw)
        else:
            d = defer.maybeDeferred(f, *args, **kw)
        self.dt.put(d)
        d.addCallbacks(ran, oops)
        d.addCallback(done)
        return d

    def getNext(self, ID):
        """
        Gets the next item for the specified prefetcherator, returning a
        deferred that fires with a response containing the pickled
        item, the isValid status indicating if the item is legit, and
        the moreLeft status indicating that at least one more item is
        left.
        """
        def bogusResponse(messageProto, *args):
            response['value'] = messageProto.format(*args)
            response['isValid'] = False
            response['moreLeft'] = False
        
        def done(result):
            response['value'] = o2p(result[0])
            response['isValid'] = result[1]
            response['moreLeft'] = result[2]

        def oops(failureObj):
            bogusResponse(
                self.info.setCall(
                    "Prefetcherator.getNext", [ID]).aboutFailure(failureObj))
        
        response = {}
        if ID not in self.pfs:
            bogusResponse("No Prefetcherator '{}'", ID)
        d = defer.maybeDeferred(self.pfs[ID].getNext)
        d.addCallbacks(done, oops)
        d.addCallback(lambda _: response)
        return d
    
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
        args = p2o(args, [])
        kw = p2o(kw, {})
        if func:
            return self.u.call(func, *args, **kw)
        text = self.info.setCall(fn, args, kw).aboutCall()
        return {
            'status': 'e',
            'result': "Couldn't identify callable for '{}'".format(text)}
    #------------------------------------------------------------------
    RunTask.responder(runTask)

    #------------------------------------------------------------------
    def getMore(self, ID):
        """
        Responder for L{GetMore}
        """
        return self.u.getNext(ID)
    #------------------------------------------------------------------
    GetMore.responder(getMore)

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


class TaskFactory(Factory):
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
    pf = TaskFactory()
    # Currently the security of UNIX domain sockets is the only
    # security we have!
    port = reactor.listenUNIX(address, pf)
    reactor.addSystemEventTrigger('before', 'shutdown', port.stopListening)
    reactor.run()


if __name__ == '__main__':
    address = sys.argv[-1]
    start(address)
    
