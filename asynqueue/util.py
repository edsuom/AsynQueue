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
Miscellaneous useful stuff.
"""

import os, signal, sys, traceback, inspect
import cPickle as pickle
import cProfile as profile

from twisted.internet import defer, reactor
from twisted.python import reflect
from twisted.python.failure import Failure

import errors, iteration


def o2p(obj):
    """
    Converts an object into a pickle string or a blank string if an
    empty container.
    """
    if isinstance(obj, (list, tuple, dict)) and not obj:
        return ""
    return pickle.dumps(obj)#, pickle.HIGHEST_PROTOCOL)

def p2o(pickledString, defaultObj=None):
    """
    Converts a pickle string into its represented object, or into the
    default object you specify if it's a blank string.

    Note that a reference to the default object itself will be
    returned, not a copy of it. So make sure you only supply an empty
    Python primitives, e.g., "[]".
    """
    if not pickledString:
        return defaultObj
    return pickle.loads(pickledString)

def callAfterDeferred(namespace, dName, f, *args, **kw):
    """
    Looks for a deferred namespace.dName and does the f-args-kw call,
    chaining its call to the deferred if necessary. Note that the
    original deferred's value is swallowed when it calls the new
    deferred's callback; the original deferred must be for signalling
    readiness only and its return value not relied upon.
    """
    def call(discarded):
        delattr(namespace, dName)
        return defer.maybeDeferred(f, *args, **kw)        
    
    d = getattr(namespace, dName, None)
    if d is None:
        return defer.maybeDeferred(f, *args, **kw)
    if d.called:
        delattr(namespace, dName)
        return defer.maybeDeferred(f, *args, **kw)
    d2 = defer.Deferred().addCallback(call)
    d.chainDeferred(d2)
    return d2


# For Testing
# ----------------------------------------------------------------------------
def testFunction(x):
    return 2*x
class TestStuff(object):
    @staticmethod
    def divide(x, y):
        return x/y
    def accumulate(self, y):
        if not hasattr(self, 'x'):
            self.x = 0
        self.x += y
        return self.x
    def setStuff(self, N1, N2):
        self.stuff = ["x"*N1] * N2
        return self
    def stufferator(self):
        for chunk in self.stuff:
            yield chunk
    def blockingTask(self, x, delay):
        import time
        time.sleep(delay)
        return 2*x
# ----------------------------------------------------------------------------
    
class CallProfiler(profile.Profile):
    def __init__(self, filename):
        self.filename = filename
        super(CallProfiler, self).__init__()

    def shutdown(self):
        self.dump_stats(self.filename)
        self.disable()


class Info(object):
    """
    I provide text (picklable) info about a call. Construct me with a
    function object and any args and keywords if you want the info to
    include that particular function call, or you can set it (and
    change it) later with L{setCall}.
    """
    def __init__(self, remember=False):
        if remember:
            self.pastInfo = {}

    def setCall(self, *metaArgs):
        """
        Sets my current f-args-kw tuple, returning a reference to myself
        to allow easy method chaining.

        The function 'f' must be an actual callable object if you want
        to use L{getWireVersion}. Otherwise it can also be a string
        depicting a callable.

        You can specify args with a second argument (as a list or
        tuple), and kw with a third argument (as a dict).
        """
        f = metaArgs[0]
        args = metaArgs[1] if len(metaArgs) > 1 else []
        kw = metaArgs[2] if len(metaArgs) > 2 else {}
        if hasattr(self, 'currentID'):
            del self.currentID
        self.callTuple = f, args, kw
        self.getID()
        return self
    
    def getID(self):
        """
        Returns a unique ID for my current callable.
        """
        def hashFAK(fak):
            fak[1] = tuple(fak[1])
            fak[2] = (
                tuple(fak[2].keys()),
                tuple(fak[2].values())) if fak[2] else None
            return hash(tuple(fak))

        if hasattr(self, 'currentID'):
            return self.currentID
        if hasattr(self, 'callTuple'):
            ID = hashFAK(list(self.callTuple))
            if hasattr(self, 'pastInfo'):
                self.pastInfo[ID] = {'callTuple': self.callTuple}
        else:
            ID = None
        self.currentID = ID
        return ID

    def forgetID(self, ID):
        """
        Use this whenever info won't be needed anymore for the specified
        call ID, to avoid memory leaks.
        """
        if ID in getattr(self, 'pastInfo', {}):
            del self.pastInfo['ID']

    def getInfo(self, ID, name):
        """
        If the supplied name is 'callTuple', returns the f-args-kw tuple
        for my current callable. The value of ID is ignored in such
        case.

        Otherwise, returns the named information attribute for the
        previous call identified with the supplied ID.
        """
        def getCallTuple():
            return getattr(self, 'callTuple', None)
        
        if hasattr(self, 'pastInfo'):
            if ID is None and name == 'callTuple':
                return getCallTuple()
            return self.pastInfo.get(ID, {}).get(name, None)
        if name == 'callTuple':
            return getCallTuple()
        return None
    
    def saveInfo(self, name, x, ID=None):
        if ID is None:
            ID = self.getID()
        if hasattr(self, 'pastInfo'):
            self.pastInfo.setdefault(ID, {})[name] = x
        return x

    def nn(self, ID=None, raw=False):
        """
        For my current callable or a previous one identified by ID,
        returns a 3-tuple namespace-ID-name combination suitable for
        sending to a process worker via pickle.

        The first element: If the callable is a method, a pickled or
        fully qualified name (FQN) version of its parent object. This
        is C{None} if the callable is a standalone function.

        The second element: If the callable is a method, the
        callable's name as an attribute of the parent object. If it's
        a standalone function, the pickled or FQN version. If nothing
        works, this element will be C{None} along with the first one.

        If the raw keyword is set True, the raw parent (or function)
        object will be returned instead of a pickle or FQN, but all
        the type checking and round-trip testing still will be done.
        """
        def strToFQN(x):
            """
            Returns the fully qualified name of the supplied string if it can
            be imported and then reflected back into the FQN, or
            C{None} if not.
            """
            try:
                obj = reflect.namedObject(x)
                fqn = reflect.fullyQualifiedName(obj)
            except:
                return
            return fqn
            
        def objToPickle(x):
            """
            Returns a string of the pickled object or C{None} if it couldn't
            be pickled and unpickled back again.
            """
            try:
                xp = o2p(x)
                p2o(xp)
            except:
                return
            return xp

        def objToFQN(x):
            """
            Returns the fully qualified name of the supplied object if it can
            be reflected into an FQN and back again, or C{None} if
            not.
            """
            try:
                fqn = reflect.fullyQualifiedName(x)
                reflect.namedObject(fqn)
            except:
                return
            return fqn

        def processObject(x):
            pickled = objToPickle(x)
            if pickled:
                return pickled
            return objToFQN(x)

        if ID:
            pastInfo = self.getInfo(ID, 'wireVersion')
            if pastInfo:
                return pastInfo
        result = None, None
        callTuple = self.getInfo(ID, 'callTuple')
        if not callTuple:
            # No callable set
            return result
        func = callTuple[0]
        if isinstance(func, (str, unicode)):
            # A callable defined as a string can only be a function
            # name, return its FQN or None if that doesn't work
            result = None, strToFQN(func)
        elif inspect.ismethod(func):
            # It's a method, so get its parent
            parent = getattr(func, 'im_self', None)
            if parent:
                processed = processObject(parent)
                if processed:
                    # Pickle or FQN of parent, method name
                    if raw:
                        processed = parent
                    result = processed, func.__name__
        if result == (None, None):
            # Couldn't get or process a parent, try processing the
            # callable itself
            processed = processObject(func)
            if processed:
                # None, pickle or FQN of callable
                if raw:
                    processed = func
                result = None, processed
        return self.saveInfo('wireVersion', result, ID)        
    
    def _divider(self, lineList):
        lineList.append(
            "-" * (max([len(x) for x in lineList]) + 1))
        
    def aboutCall(self, ID=None):
        """
        Returns an informative string describing my current function call
        or a previous one identified by ID.
        """
        if ID:
            pastInfo = self.getInfo(ID, 'aboutCall')
            if pastInfo:
                return pastInfo
        callTuple = self.getInfo(ID, 'callTuple')
        if not callTuple:
            return ""
        func, args, kw = callTuple
        if isinstance(func, (str, unicode)):
            text = func
        elif func.__class__.__name__ == "function":
            text = func.__name__
        elif callable(func):
            text = "{}.{}".format(func.__class__.__name__, func.__name__)
        else:
            try:
                func = str(func)
            except:
                func = repr(func)
            text = "{}[Not Callable!]".format(func)
        text += "("
        if args:
            text += ", ".join([str(x) for x in args])
        for name, value in kw.iteritems():
            text += ", {}={}".format(name, value)
        text += ")"
        return self.saveInfo('aboutCall', text, ID)
    
    def aboutException(self, ID=None, exception=None):
        """
        Returns an informative string describing an exception raised from
        my function call or a previous one identified by ID, or one
        you supply (as an instance, not a class).
        """
        if ID:
            pastInfo = self.getInfo(ID, 'aboutException')
            if pastInfo:
                return pastInfo
        if exception:
            lineList = ["Exception '{}'".format(repr(exception))]
        else:
            stuff = sys.exc_info()
            lineList = ["Exception '{}'".format(stuff[1])]
        callInfo = self.aboutCall()
        if callInfo:
            lineList.append(
                " doing call '{}':".format(callInfo))
        self._divider(lineList)
        if not exception:
            lineList.append("".join(traceback.format_tb(stuff[2])))
            del stuff
        text = "\n".join(lineList)
        return self.saveInfo('aboutException', text, ID)

    def aboutFailure(self, failureObj, ID=None):
        """
        Returns an informative string describing a Twisted failure raised
        from my function call or a previous one identified by ID. You
        can use this as an errback.
        """
        if ID:
            pastInfo = self.getInfo(ID, 'aboutFailure')
            if pastInfo:
                return pastInfo
        lineList = ["Failure '{}'".format(failureObj.getErrorMessage())]
        callInfo = self.aboutCall()
        if callInfo:
            lineList.append(
                " doing call '{}':".format(callInfo))
        self._divider(lineList)
        lineList.append(failureObj.getTraceback(detail='verbose'))
        text = "\n".join(lineList)
        return self.saveInfo('aboutFailure', text, ID)


class DeferredTracker(object):
    """
    I allow you to track and wait for deferreds without actually having
    received a reference to them.
    """
    def __init__(self):
        self.dList = []
    
    def put(self, d):
        """
        Put another deferred in the tracker.
        """
        def transparentCallback(anything):
            if d in self.dList:
                self.dList.remove(d)
            return anything

        d.addBoth(transparentCallback)
        if not isinstance(d, defer.Deferred):
            raise TypeError("Object {} is not a deferred".format(repr(d)))
        self.dList.append(d)
        return d

    def deferToAll(self):
        """
        Return a deferred that tracks all active deferreds that haven't
        yet fired. When the tracked deferreds fire, the returned
        deferred fires, too.
        """
        if self.dList:
            d = defer.DeferredList(self.dList)
            self.dList = []
        elif hasattr(self, 'd_WFA') and not self.d_WFA.called():
            d = defer.Deferred()
            self.d_WFA.chainDeferred(d)
        else:
            d = defer.succeed(None)
        return d

    def deferToLast(self):
        """
        Return a deferred that tracks the deferred that was most recently put
        in the tracker. When the tracked deferred fires, the returned deferred
        fires, too.
        """
        if self.dList:
            d = defer.Deferred()
            self.dList.pop().chainDeferred(d)
        elif hasattr(self, 'd_WFL') and not self.d_WFL.called():
            d = defer.Deferred()
            self.d_WFL.chainDeferred(d)
        else:
            d = defer.succeed(None)
        return d


class DeferredLock(defer.DeferredLock):
    """
    I am a modified form of L{defer.DeferredLock lock that lets you
    shut things down when you get the lock.

    Raises an exception if you try to acquire the lock after a
    shutdown has been initated.

    """
    def __init__(self):
        self.N_vips = 0
        self.stoppers = []
        self.running = True
        super(DeferredLock, self).__init__()

    def acquire(self, vip=False):
        """
        Like L{defer.DeferredLock.acquire} except with a vip option. That
        lets you cut ahead of everyone in the regular waiting list and
        gets the next lock, after anyone else in the VIP line who is
        waiting from their own call of this method.
        """
        def transparentCallback(result):
            self.N_vips -= 1
            return result
        
        if not self.running:
            raise errors.QueueRunError
        d = defer.Deferred(canceller=self._cancelAcquire)
        if self.locked:
            if vip:
                d.addCallback(transparentCallback)
                self.waiting.insert(self.N_vips, d)
                self.N_vips += 1
            else:
                self.waiting.append(d)
        else:
            self.locked = True
            d.callback(self)
        return d
    
    def addStopper(self, f, *args, **kw):
        """
        Add a callable (along with any args and kw) to be run when
        shutting things down. The callable may return a deferred, and
        more than one can be added. They will be called, and their
        result awaited, in the order received.

        """
        self.stoppers.append([f, args, kw])
    
    def stop(self):
        """
        Shut things down, when the waiting list empties.
        """
        @defer.inlineCallbacks
        def runStoppers(me):
            while self.stoppers:
                f, args, kw = self.stoppers.pop(0)
                yield defer.maybeDeferred(f, *args, **kw)
            me.release()
                
        self.running = False
        return super(DeferredLock, self).acquire().addCallback(runStoppers)
    

class CallRunner(object):
    """
    Call me with a callTuple to get a 2-tuple containing the status of
    the call and its result:

    'e': An exception was raised; the result is a pretty-printed
         traceback string.

    'r': Ran fine, the result is the return value of the call.

    'i': Ran fine, but the result is an iterable other than a standard
         Python one.

    """
    def __init__(self):
        self.info = Info()

    def __call__(self, callTuple):
        f, args, kw = callTuple
        try:
            result = f(*args, **kw)
            # If the task causes the thread to hang, the method
            # call will not reach this point.
        except Exception as e:
            return ('e', self.info.setCall(f, args, kw).aboutException())
        if iteration.Deferator.isIterator(result):
            return ('i', result)
        return ('r', result)

        

