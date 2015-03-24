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

import traceback

import cPickle as pickle

import errors


def callInfo(func, *args, **kw):
    """
    Returns an informative string describing a function call.
    """
    if func.__class__.__name__ == "function":
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
        text += ", ".join(args)
    for name, value in kw.iteritems():
        text += ", {}={}".format(name, value)
    text += ")"
    return text


def callTraceback(e, func, *args, **kw):
    """
    Returns an informative string describing an exception raised from
    a function call.
    """
    lineList = [
        "Exception '{}'".format(str(e)),
        " doing call '{}':".format(callInfo(func, *args, **kw))]
    lineList.append(
        "-" * (max([len(x) for x in lineList]) + 1))
    lineList.append("".join(traceback.format_tb(e[2])))
    return "\n".join(lineList)


def o2p(obj):
    """
    Converts an object into a pickle string or a blank string if an
    empty container.
    """
    if not obj:
        return ""
    return pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)


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


class Deferator(object):
    """
    Use an instance of me in place of a task result that is an
    iterable other than one of Python's built-in containers (list,
    dict, etc.). I yield deferreds to the next iteration of the
    result.

    When the deferred from my first L{next} call fires, with the first
    iteration of the underlying (possibly remote) iterable, you can
    call L{next} again to get a deferred to the next one, and so on,
    until I raise L{StopIteration} just like a regular iterable.

    You MUST wrap my iteration in a L{defer.inlineCallbacks} loop or
    otherwise wait for each yielded deferred to fire before asking for
    the next one. Something like this:

    @defer.inlineCallbacks
    def printItems(self, ID):
        for d in Deferator("remoteIterator", getMore, ID):
            listItem = yield d
            print listItem

    Instantiate me with the string representation of the underlying
    iterable and a function (along with any args and kw) that returns
    a deferred to (1) the next value yielded from the task result,
    and (2) a Bool indicating whether there are more iterations left.

    This requires your get-more function to be one step ahead somehow,
    returning C{False} as its status indicator when the *next* call
    would raise L{StopIteration}.

    """
    builtIns = (
        str, unicode,
        list, tuple, bytearray, buffer, dict, set, frozenset)
    
    @classmethod
    def isIterator(cls, obj):
        """
        Returns C{True} if the object is an iterator suitable for use with
        me, C{False} otherwise.
        """
        if isinstance(obj, cls.builtIns):
            return False
        try:
            iter(obj)
        except:
            return True
        return False

    def __init__(self, representation, f, *args, **kw):
        self.representation = representation.strip('<>')
        self.callTuple = f, args, kw 
        self.moreLeft = True

    def __repr__(self):
        return "<Deferator wrapping of <{}>, at {}>".format(
            self.representation, id(self))

    def __iter__(self):
        return self
        
    def next(self):
        def gotNext(result):
            value, self.moreLeft = result
            return value
        
        if self.moreLeft:
            if hasattr(self, 'd') and not self.d.called:
                raise errors.NotReadyError(
                    "You didn't wait for the last deferred to fire!")
            f, args, kw = self.callTuple
            return f(*args, **kw).addCallback(gotNext)
        raise StopIteration


class DeferredLock(defer.DeferredLock):
    """
    I am a modified form of L{defer.DeferredLock lock that lets you
    shut things down when you get the lock.

    Raises an exception if you try to acquire the lock after a
    shutdown has been initated.

    """
    def __init__(self):
        self.running = True

    def acquireNext(self):
        """
        Like L{defer.DeferredLock.acquire} except cuts ahead of everyone
        else in the waiting list and gets the next lock (unless
        someone else cuts ahead again, with another call of this
        method).
        """
        if not self.running:
            raise errors.QueueRunError
        d = defer.Deferred(canceller=self._cancelAcquire)
        if self.locked:
            self.waiting.insert(0, d)
        else:
            self.locked = True
            d.callback(self)
        return d

    def acquire(self):
        if not self.running:
            raise errors.QueueRunError
        return super(DeferredLock, self).acquire()
    
    def addStopper(self, f, *args, **kw):
        """
        Add a callable (along with any args and kw) to be run when
        shutting things down. The callable may return a deferred, and
        more than one can be added.
        """
        if not hasattr(self, stoppers):
            self.stoppers = []
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


class ThreadLooper(object):
    """
    I run function calls in a dedicated worker thread, returning a
    deferred to the eventual result.

    If the result is an iterable other than one of Python's built-in
    ones, the deferred fires with an instance of L{Deferator}
    instead. Each of its iterations corresponds to an iteration that
    runs in my thread on the underlying iterable.

    My statusResult attribute is a 2-tuple containing the status of
    the last call and its result:

    'e': An exception was raised; the result is a pretty-printed
      traceback string.

    'r': Ran fine, the result is the return value of the call.

    'i': Ran fine, but the result is an iterable other than a standard
      Python one. The result is an instance of L{Deferator}.

    """
    def __init__(self):
        import threading
        self.lock = DeferredLock()
        self.event = threading.Event()
        self.thread = threading.Thread(target=self.loop)
        self.thread.start()

    def getNext(self):
        """
        Gets the next value from my current iterator, returning it along
        with a Bool indicating if more values are left.
        """
        if not hasatttr(self, 'iterator'):
            # The iterator has been deleted, so what we've already got
            # is all we're going to get.
            return getattr(self, 'nextValue', None), False
        # We still have an iterator,
        try:
             # ...let's see if it has another value left
            value = self.iterator.next()
        except:
            # Nope, it's empty. Delete the iterator and use the
            # previous "next" value, if any. Note that, unfortunately,
            # this will guarantee a single None value is yielded from
            # iterators that were empty to begin with.
            del self.iterator
            moreLeft = False
        else:
            # Yes! Now let's try prefetching the next value to see if
            # this call can be made again.
            try:
                # Is there one left beyond the current one?
                nextValue = self.iterator.next()
            except:
                # Nope, what we have is all we are going to get.
                del self.iterator
                del self.nextValue
                moreLeft = False
            else:
                # Yes! We will need to save this next value for the
                # next call.
                moreLeft = True
                self.nextValue = nextValue
        # If we have a "next" value from a previous call to this
        # method, that is our current value and our current one
        if hasattr(self, 'nextValue'):
            value = self.nextValue
        return value, moreLeft

    def loop(self):
        """
        Runs a loop in a dedicated thread that waits for new tasks. The loop
        exits when a C{None} object is supplied as a task.
        """
        while True:
            # Wait here on the threading.Event object
            self.event.wait()
            if self.callTuple is None:
                # Shutdown was requested
                break
            f, args, kw = self.callTuple
            # Ready for another calltuple to be set
            self.event.clear()
            try:
                result = f(*args, **kw)
                # If the task causes the thread to hang, the method
                # call will not reach this point.
            except Exception as e:
                status = 'e'
                result = callTraceback(e, f, *args, **kw)
            else:
                if Deferator.isIterator(result):
                    # An iterator
                    if hasattr(self, 'iterator'):
                        raise error.NotReadyError(
                            "We already have an iterator in progress")
                    self.iterator = iter(result)
                    status = 'i'
                    result = Deferator(
                        repr(result), self.deferToThread, self.getNext)
                else:
                    status = 'r'
            reactor.callFromThread(self.d.callback, (status, result))
        # Broken out of loop, ready for the thread to end
        reactor.callFromThread(self.lock.release)
        # The thread now dies

    def deferToThread(self, f, *args, **kw):
        def threadReady(null):
            self.callTuple = f, args, kw
            self.event.set()
            self.d = defer.Deferred().addCallback(threadDone)
            return self.d

        def threadDone(result):
            self.lock.release()
            return result

        return self.lock.aquire().addCallback(threadReady)
    
    def stop(self):
        """
        The returned deferred fires when the task loop has ended and its
        thread terminated.
        """
        def stopper():
            self.quit = True
        

        def stopper():
            self.d = defer.Deferred()
            # Kill the thread when it quits its loop and fires this deferred
            self.d.addCallback(lambda _ : self.thread.join())
            # Tell the thread to quit with a null task
            self.task = None
            self.event.set()
            return self.d
        return self.deferToStop(stopper)

