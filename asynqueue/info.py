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
Information about callables and what happens to them.
"""

import cPickle as pickle
import sys, traceback, inspect
from contextlib import contextmanager

from twisted.internet import defer
from twisted.python import reflect


def hashIt(*args):
    """
    Returns a pretty much unique 32-bit hash for pretty much any
    python object.
    """
    total = 0L
    for x in args:
        if isinstance(x, dict):
            for k, key in enumerate(sorted(x.keys())):
                total += hashIt(k, key, x[key])
        elif isinstance(x, (list, tuple)):
            for k, value in enumerate(x):
                total += hashIt(k, value)
        else:
            total += hash(x)
    return hash(total)


SR_STUFF = [0, None]
def showResult(f):
    """
    Use as a decorator to print info about the function and its
    result. Follows deferred results.
    """
    def substitute(self, *args, **kw):
        def msg(result, callInfo):
            resultInfo = str(result)
            if len(callInfo) + len(resultInfo) > 70:
                callInfo += "\n"
            print "\n{} -> {}".format(callInfo, resultInfo)
            return result

        SR_STUFF[0] += 1
        callInfo = "{:03d}: {}".format(
            SR_STUFF[0],
            SR_STUFF[1].setCall(
                instance=self, args=args, kw=kw).aboutCall())
        result = f(self, *args, **kw)
        if isinstance(result, defer.Deferred):
            return result.addBoth(msg, callInfo)
        return msg(result, callInfo)

    SR_STUFF[1] = Info().setCall(f)
    substitute.func_name = f.func_name
    return substitute


class Converter(object):
    """
    I provide a bunch of methods for converting objects.
    """
    def strToFQN(self, x):
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
        
    def objToPickle(self, x):
        """
        Returns a string of the pickled object or C{None} if it couldn't
        be pickled and unpickled back again.
        """
        try:
            xp = pickle.dumps(x)
            pickle.loads(xp)
        except:
            return
        return xp

    def objToFQN(self, x):
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

    def processObject(self, x):
        """
        Attempts to convert the supplied object to a pickle and, failing
        that, to a fully qualified name.
        """
        pickled = self.objToPickle(x)
        if pickled:
            return pickled
        return self.objToFQN(x)

    
class InfoHolder(object):
    """
    An instance of me is yielded by L{Info.context}, for you to call
    about info concerning a particular saved function call.
    """
    def __init__(self, info, ID):
        self.info = info
        self.ID = ID
    def getInfo(self, name):
        return self.info.getInfo(self.ID, name)
    def nn(self, raw=False):
        return self.info.nn(self.ID, raw)
    def aboutCall(self):
        return self.info.aboutCall(self.ID)
    def aboutException(self, exception=None):
        return self.info.aboutCall(self.ID, exception)
    def aboutFailure(self, failureObj):
        return self.info.aboutFailure(failureObj, self.ID)


class Info(object):
    """
    I provide text (picklable) info about a call. Construct me with a
    function object and any args and keywords if you want the info to
    include that particular function call, or you can set it (and
    change it) later with L{setCall}.
    """
    def __init__(self, remember=False):
        self.cv = Converter()
        self.lastMetaArgs = None
        if remember:
            self.pastInfo = {}

    def setCall(self, *metaArgs, **kw):
        """
        Sets my current f-args-kw tuple, returning a reference to myself
        to allow easy method chaining.

        The function 'f' must be an actual callable object if you want
        to use L{getWireVersion}. Otherwise it can also be a string
        depicting a callable.

        You can specify args with a second argument (as a list or
        tuple), and kw with a third argument (as a dict). If you are
        only specifying a single arg, you can just provide it as your
        second argument to this method call without wrapping it in a
        list or tuple. I try to be flexible.

        If you've set a function name and want to add a sequence of
        args or a dict of keywords, you can do it by supplying the
        'args' or 'kw' keywords. You can also set a class instance at
        that time with the 'instance' keyword.
        """
        if metaArgs:
            if metaArgs == self.lastMetaArgs and not hasattr(self, 'pastInfo'):
                # We called this already with the same metaArgs and
                # without any pastInfo to reckon with, so there's
                # nothing to do.
                return self
            # Starting over with a new f
            callDict = {'f': metaArgs[0], 'fs': self._funcText(metaArgs[0])}
            args = metaArgs[1] if len(metaArgs) > 1 else []
            if not isinstance(args, (tuple, list)):
                args = [args]
            callDict['args'] = args
            callDict['kw'] = metaArgs[2] if len(metaArgs) > 2 else {}
            callDict['instance'] = None
            self.callDict = callDict
        elif hasattr(self, 'callDict'):
            # Adding to an existing f
            for name in ('args', 'kw', 'instance'):
                if name in kw:
                    self.callDict[name] = kw[name]
        else:
            raise ValueError(
                "You must supply at least a new function/string "+\
                "or keywords adding args, kw to a previously set one")
        if hasattr(self, 'currentID'):
            del self.currentID
        self.ID
        if metaArgs:
            # Save metaArgs to ignore repeated calls with the same metaArgs
            self.lastMetaArgs = metaArgs
        return self

    @property
    def ID(self):
        """
        Returns a unique ID for my current callable.
        """
        if hasattr(self, 'currentID'):
            return self.currentID
        if hasattr(self, 'callDict'):
            thisID = hashIt(self.callDict)
            if hasattr(self, 'pastInfo'):
                self.pastInfo[thisID] = {'callDict': self.callDict}
        else:
            thisID = None
        self.currentID = thisID
        return thisID

    def forgetID(self, ID):
        """
        Use this whenever info won't be needed anymore for the specified
        call ID, to avoid memory leaks.
        """
        if ID in getattr(self, 'pastInfo', {}):
            del self.pastInfo[ID]

    @contextmanager
    def context(self, *metaArgs, **kw):
        """
        Call this context manager method with info about a particular call
        (same format as L{setCall} uses) and it yields an
        L{InfoHolder} object keyed to that call. It lets you get info
        about the call inside the context, without worrying about the
        ID or calling L{forgetID}, even after I have been used for
        other calls outside the context.
        """
        if not hasattr(self, 'pastInfo'):
            raise Exception(
                "Can't use a context manager without saving call info")
        ID = self.setCall(*metaArgs, **kw).ID
        yield InfoHolder(self, ID)
        self.forgetID(ID)
            
    def getInfo(self, ID, name, nowForget=False):
        """
        If the supplied name is 'callDict', returns the f-args-kw-instance
        dict for my current callable. The value of ID is ignored in
        such case. Otherwise, returns the named information attribute
        for the previous call identified with the supplied ID.

        Set 'nowForget' to remove any reference to this ID or
        callDict after the info is obtained.
        """
        def getCallDict():
            if hasattr(self, 'callDict'):
                result = self.callDict
                if nowForget:
                    del self.callDict
            else:
                result = None
            return result
        
        if hasattr(self, 'pastInfo'):
            if ID is None and name == 'callDict':
                return getCallDict()
            if ID in self.pastInfo:
                x = self.pastInfo[ID]
                if nowForget:
                    del self.pastInfo[ID]
                return x.get(name, None)
            return None
        if name == 'callDict':
            return getCallDict()
        return None
    
    def saveInfo(self, name, x, ID=None):
        if ID is None:
            ID = self.ID
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

        if ID:
            pastInfo = self.getInfo(ID, 'wireVersion')
            if pastInfo:
                return pastInfo
        result = None, None
        callDict = self.getInfo(ID, 'callDict')
        if not callDict:
            # No callable set
            return result
        func = callDict['f']
        if isinstance(func, (str, unicode)):
            # A callable defined as a string can only be a function
            # name, return its FQN or None if that doesn't work
            result = None, self.cv.strToFQN(func)
        elif inspect.ismethod(func):
            # It's a method, so get its parent
            parent = getattr(func, 'im_self', None)
            if parent:
                processed = self.cv.processObject(parent)
                if processed:
                    # Pickle or FQN of parent, method name
                    if raw:
                        processed = parent
                    result = processed, func.__name__
        if result == (None, None):
            # Couldn't get or process a parent, try processing the
            # callable itself
            processed = self.cv.processObject(func)
            if processed:
                # None, pickle or FQN of callable
                if raw:
                    processed = func
                result = None, processed
        return self.saveInfo('wireVersion', result, ID)        
    
    def _divider(self, lineList):
        N_dashes = max([len(x) for x in lineList]) + 1
        if N_dashes > 79:
            N_dashes = 79
        lineList.append("-" * N_dashes)

    def _formatList(self, lineList):
        lines = []
        for line in lineList:
            newLines = line.split(':')
            for newLine in newLines:
                for reallyNewLine in newLine.split('\\n'):
                    lines.append(reallyNewLine)
        return "\n".join(lines)
    
    def _funcText(self, func):
        if isinstance(func, (str, unicode)):
            return func
        if callable(func):
            text = func.__name__
            if inspect.ismethod(func):
                text = "{}.{}".format(func.im_self, text)
            return text
        try:
            func = str(func)
        except:
            func = repr(func)
        return "{}[Not Callable!]".format(func)
        
    def aboutCall(self, ID=None, nowForget=False):
        """
        Returns an informative string describing my current function call
        or a previous one identified by ID.
        """
        if ID:
            pastInfo = self.getInfo(ID, 'aboutCall', nowForget)
            if pastInfo:
                return pastInfo
        callDict = self.getInfo(ID, 'callDict')
        if not callDict:
            return ""
        func, args, kw = [callDict[x] for x in ('f', 'args', 'kw')]
        instance = callDict.get('instance', None)
        text = repr(instance) + "." if instance else ""
        text += self._funcText(func) + "("
        if args:
            text += ", ".join([str(x) for x in args])
        for name, value in kw.iteritems():
            text += ", {}={}".format(name, value)
        text += ")"
        return self.saveInfo('aboutCall', text, ID)
    
    def aboutException(self, ID=None, exception=None, nowForget=False):
        """
        Returns an informative string describing an exception raised from
        my function call or a previous one identified by ID, or one
        you supply (as an instance, not a class).
        """
        if ID:
            pastInfo = self.getInfo(ID, 'aboutException', nowForget)
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
        text = self._formatList(lineList)
        return self.saveInfo('aboutException', text, ID)

    def aboutFailure(self, failureObj, ID=None, nowForget=False):
        """
        Returns an informative string describing a Twisted failure raised
        from my function call or a previous one identified by ID. You
        can use this as an errback.
        """
        if ID:
            pastInfo = self.getInfo(ID, 'aboutFailure', nowForget)
            if pastInfo:
                return pastInfo
        lineList = ["Failure '{}'".format(failureObj.getErrorMessage())]
        callInfo = self.aboutCall()
        if callInfo:
            lineList.append(
                " doing call '{}':".format(callInfo))
        self._divider(lineList)
        lineList.append(failureObj.getTraceback(detail='verbose'))
        text = self._formatList(lineList)
        return self.saveInfo('aboutFailure', text, ID)
