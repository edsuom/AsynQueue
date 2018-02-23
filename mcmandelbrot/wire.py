#!/usr/bin/env python
#
# mcmandelbrot
#
# An example package for AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
#
# Copyright (C) 2015 by Edwin A. Suominen,
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
Uses C{asynqueue.wire} to run and communicate with a server that
generates Mandelbrot Set images.

For the server end, use L{server} to get a Twisted C{service} object
you can start or add to an C{application}.

To communicate with the server, use L{RemoteRunner} to get an
AsynQueue C{Worker} that you can add to your C{TaskQueue}.

Both ends of the connection need to be able to import this module and
reference its L{MandelbrotWorkerUniverse} class.
"""

import sys, urlparse
from collections import namedtuple

from zope.interface import implements
from twisted.application import internet, service
from twisted.internet import defer, reactor
from twisted.internet.interfaces import IConsumer

from asynqueue import iteration
from asynqueue.base import TaskQueue
from asynqueue.wire import \
    WireWorkerUniverse, WireWorker, WireServer, ServerManager

from mcmandelbrot import runner


# Server Connection defaults
PORT = 1978
INTERFACE = None
DESCRIPTION = None
VERBOSE = True

FQN = "mcmandelbrot.wire.MandelbrotWorkerUniverse"


class Writable(object):
    delay = iteration.Delay()
    
    def __init__(self):
        self.ID = str(hash(self))
        self.data = []
    
    def write(self, data):
        self.data.append(data)

    def close(self):
        self.data.append(None)
        
    def getNext(self):
        def haveData(really):
            if really:
                return self.data.pop(0)
        return self.delay.untilEvent(
            lambda: bool(self.data)).addCallback(haveData)
        

class MandelbrotWorkerUniverse(WireWorkerUniverse):
    """
    I run on the remote Python interpreter to run a runner from there,
    accepting commands from your L{RemoteRunner} via L{run},
    L{done} and L{cancel} and sending the results and iterations
    to it.
    """
    RunInfo = namedtuple('RunInfo', ['fh', 'dRun', 'dCancel'])

    def setup(self, N_values, steepness):
        def ready(null):
            # These methods are called via a one-at-a-time queue, so
            # it's not a problem to overwrite the old runner with a
            # new one, now that we've waited for the old one's runs to
            # get canceled and then for it to shut down.
            self.pendingRuns = {}
            self.runner = runner.Runner(N_values, steepness, verbose=VERBOSE)
            self.triggerID = reactor.addSystemEventTrigger(
                'before', 'shutdown', self.shutdown)
        return self.shutdown().addCallback(ready)

    @defer.inlineCallbacks
    def shutdown(self):
        if hasattr(self, 'triggerID'):
            reactor.removeSystemEventTrigger(self.triggerID)
            del self.triggerID
        if hasattr(self, 'runner'):
            dList = []
            for ri in self.pendingRuns.itervalues():
                dList.append(ri.dRun)
                if not ri.dCancel.called:
                    ri.dCancel.callback(None)
            yield defer.DeferredList(dList)
            yield self.runner.q.shutdown()

    def run(self, Nx, cr, ci, crPM, ciPM, requester=None):
        """
        Does an image-generation run for the specified parameters, storing
        a C{Deferred} I{dRun} to the result in a C{namedtuple} along
        with a reference I{fh} to a new L{Writable} that will have the
        image data written to it and a C{Deferred} I{dCancel} that can
        have its callback fired to cancel the run.

        @return: A unique string I{ID} identifying the run.
        """
        def doneHere(stuff):
            fh.close()
            return stuff

        fh = Writable()
        dCancel = defer.Deferred()
        dRun = self.runner.run(
            fh, Nx, cr, ci, crPM, ciPM,
            dCancel=dCancel, requester=requester).addCallback(doneHere)
        self.pendingRuns[fh.ID] = self.RunInfo(fh, dRun, dCancel)
        return fh.ID

    def getNext(self, ID):
        """
        Gets the next chunk of data to have been written to the
        L{Writable} for the run identified by I{ID}.

        @return: A C{Deferred} that fires with the data chunk when it
          is received, or an immediate C{None} object if the run isn't
          pending.
        """
        if ID in self.pendingRuns:
            return self.pendingRuns[ID].fh.getNext()
        
    def done(self, ID):
        """
        Gets the runtime and number of points done in the run and removes
        my references to it in my I{pendingRuns} dict.

        @return: A C{Deferred} that fires with the runtime and number
          of points when the run is done, or an immediate C{None}
          object if there never was any such run or this method was
          called with this I{ID} already.
        """
        if ID in self.pendingRuns:
            return self.pendingRuns.pop(ID).dRun
        
    def cancel(self, ID):
        """
        Cancels a pending run I{ID}. Nothing is returned, and no exception
        is raised for calling with reference to an unknown or already
        canceled/completed run.
        """
        if ID in self.pendingRuns:
            dCancel = self.pendingRuns[ID].dCancel
            if not dCancel.called:
                dCancel.callback(None)


class RemoteRunner(object):
    """
    Call L{setup} and wait for the C{Deferred} it returns, then you
    can call L{image} as much as you like to get images streamed to
    you as iterations of C{Deferred} chunks.

    Call L{shutdown} when done, unless you are using both a remote
    server and an external instance of C{TaskQueue}.
    """
    setupDefaults = {'N_values': 3000, 'steepness': 3}
    
    def __init__(self, description=None, q=None):
        self.description = description
        self.sv = self.setupDefaults.copy()
        if q is None:
            self.q = TaskQueue()
            self.stopper = self.q.shutdown
        else:
            self.q = q
    
    @defer.inlineCallbacks
    def setup(self, **kw):
        """
        Call at least once to set things up. Repeated calls with the same
        keywords, or with no keywords, do nothing. Keywords with a
        value of C{None} are ignored.

        @keyword N_values: The number of possible values for each
          iteration.

        @keyword steepness: The steepness of the exponential applied to
          the value curve.

        @keyword worker: A custom worker to use instead of
          C{asynqueue.wire.WireWorker}.
        
        @return: A C{Deferred} that fires when things are setup, or
          immediately if they already are as specified.
        """
        def checkSetup():
            result = False
            if 'FLAG' not in self.sv:
                self.sv['FLAG'] = True
                result = True
            for name, value in kw.iteritems():
                if value is None:
                    continue
                if self.sv.get(name, None) != value:
                    self.sv.update(kw)
                    return True
            return result
        
        if checkSetup():
            if 'worker' in kw:
                worker = kw.pop('worker')
            else:
                if self.description is None:
                    # Local server running on a UNIX socket. Mostly
                    # useful for testing.
                    self.mgr = ServerManager(FQN)
                    description = self.mgr.newSocket()
                    yield self.mgr.spawn(description)
                else:
                    description = self.description
                wwu = MandelbrotWorkerUniverse()
                worker = WireWorker(wwu, description, series=['mcm'])
            yield self.q.attachWorker(worker)
            yield self.q.call(
                'setup',
                self.sv['N_values'], self.sv['steepness'], series='mcm')

    @defer.inlineCallbacks
    def shutdown(self):
        if hasattr(self, 'mgr'):
            yield self.mgr.done()
        if hasattr(self, 'stopper'):
            yield self.stopper()

    @defer.inlineCallbacks
    def run(self, fh, Nx, cr, ci, crPM, ciPM, dCancel=None, requester=None):
        """
        Runs a C{compute} method on a remote Python interpreter to
        generate a PNG image of the Mandelbrot Set and write it in
        chunks, indirectly, to the write-capable object I{fh}, which
        in this case must implement C{IConsumer}. When this method is
        called by L{image.Imager.renderImage}, I{fh} will be a request
        and those do implement C{IConsumer}.

        The image is centered at location I{cr, ci} in the complex
        plane, plus or minus I{crPM} on the real axis and I{ciPM} on
        the imaginary axis.

        @see: L{runner.Runner.run}.

        This method doesn't call L{setup}; that is taken care of by
        L{image.Imager} for HTTP requests and by L{writeImage} for
        local image file generation.

        @return: A C{Deferred} that fires with the total elasped time
          for the computation and the number of pixels computed.
        """
        def canceler(null, ID):
            return self.q.call('cancel', ID, niceness=-15, series='mcm')
        
        ID = yield self.q.call(
            'run', Nx, cr, ci, crPM, ciPM, requester=requester, series='mcm')
        if dCancel:
            dCancel.addCallback(canceler, ID)
        while True:
            chunk = yield self.q.call('getNext', ID, series='mcm', raw=True)
            if chunk is None:
                break
            fh.write(chunk)
        runInfo = yield self.q.call('done', ID)
        defer.returnValue(runInfo)

    @defer.inlineCallbacks
    def writeImage(self, fileName, *args, **kw):
        """
        Call with the same arguments as L{run} after I{fh}, preceded by a
        writable I{fileName}. It will be opened for writing and its
        file handle supplied to L{run} as I{fh}.

        Writes the PNG image as it is generated remotely, returning a
        C{Deferred} that fires with the result of L{run} when the
        image is all written.

        @see: L{setup} and L{run}
        """
        N_values = kw.pop('N_values', None)
        steepness = kw.pop('steepness', None)
        yield self.setup(N_values=N_values, steepness=steepness)
        fh = open(fileName, 'w')
        runInfo = yield self.run(fh, *args)
        fh.close()
        defer.returnValue(runInfo)

    def showStats(self, runInfo):
        proto = "Remote server computed {:d} pixels in {:1.1f} seconds."
        print proto.format(runInfo[1], runInfo[0])
        return defer.succeed(None)


def server(description=None, port=1978, interface=None):
    """
    Creates a Twisted C{endpoint} service for Mandelbrot Set images.

    The returned C{service} responds to connections as specified by
    I{description} and accepts 'image' commands via AMP to produce PNG
    images of the Mandelbrot set in a particular region of the complex
    plane.

    If you omit the I{description}, it will be a TCP server running on
    a particular I{port}. The default is C{1978}, which is the year in
    which the first computer image of the Mandelbrot set was
    generated. You can specify an I{interface} dotted-quad address for
    the TCP server if you want to limit connections that way.

    @see: L{MandelbrotWorkerUniverse.run}
    """
    if description is None:
        description = b"tcp:{:d}".format(port)
        if interface:
            description += ":interface={}".format(interface)
    mwu = MandelbrotWorkerUniverse()
    ws = WireServer(mwu)
    return ws.run(description)


if '/twistd' in sys.argv[0]:
    application = service.Application("Mandelbrot Set PNG Image Server")
    server(DESCRIPTION, PORT, INTERFACE).setServiceParent(application)

    
