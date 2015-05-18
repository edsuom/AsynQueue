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
An example of C{AsynQueue} in action. Can be fun to play with if you
have a multicore CPU. You will need the following packages, which you
can get via C{pip install}:

  - C{weave} (part of SciPy)
  - C{numpy} (part of SciPy)
  - C{matplotlib}
  - C{asynqueue} (Duh...)


Command line usage::

  mcmandelbrot
    [-s steepness] [-N values] [-o imageFile]
    N cr ci crPM [ciPM]

Produces chunks of a PNG image, to stdout if you don't specify an
imageFile with the C{-o} option.

Example: C{mcm 2000 -0.630 0 1.4 1.2 >overview.png}

"""

import sys, time, array

import png
import numpy as np

from zope.interface import implements
from twisted.internet import defer, reactor
from twisted.internet.interfaces import IPushProducer

import asynqueue
from asynqueue.threads import OrderedItemProducer, Filerator

from valuer import MandelbrotValuer


class Runner(object):
    """
    I run a multi-process Mandelbrot Set computation operation.

    @cvar N_processes: The number of processes to use, disregarded if
      I{useThread} is set C{True} in my constructor.
    """
    def __init__(self, N_values, steepness, stats=False):
        self.q = asynqueue.ProcessQueue(self.N_processes, callStats=stats)
        self.mv = MandelbrotValuer(N_values, steepness)

    def shutdown(self):
        return self.q.shutdown()
        
    @property
    def N_processes(self):
        maxValue = asynqueue.ProcessQueue.cores() - 1
        return max([1, maxValue])
    
    def run(self, fh, Nx, cr, ci, crPM, ciPM, dCancel=None):
        """
        Runs my L{compute} method to generate a PNG image of the
        Mandelbrot Set and write it in chunks to the file handle or
        write-capable object I{fh}.

        The image is centered at location I{cr, ci} in the complex
        plane, plus or minus I{crPM} on the real axis and I{ciPM on
        the imaginary axis.

        @return: A C{Deferred} that fires with the total elasped time
          for the computation and the number of pixels computed.
        """
        def diff(k):
            return xySpans[k][1] - xySpans[k][0]
        def done(N):
            return time.time() - t0, N

        t0 = time.time()
        xySpans = []
        for center, plusMinus in ((cr, crPM), (ci, ciPM)):
            xySpans.append([center - plusMinus, center + plusMinus])
        xySpans[0].append(Nx)
        xySpans[1].append(int(Nx * diff(1) / diff(0)))
        return self.compute(
            fh, xySpans[0], xySpans[1], dCancel).addCallback(done)
        
    @defer.inlineCallbacks
    def compute(self, fh, xSpan, ySpan, dCancel=None):
        """
        Computes the Mandelbrot Set under C{Twisted} and generates a
        pretty image, written as a PNG image to the supplied file
        handle I{fh} one row at a time.

        @return: A C{Deferred} that fires when the image is completely
          written and you can close the file handle, with the number
          of pixels computed (may be a lower number than expected if
          the connection terminated early).
        """
        def f(rows):
            print "F", rows
            try:
                writer = png.Writer(Nx, Ny, bitdepth=8, compression=9)
                writer.write(fh, rows)
            except:
                # Trap ValueError caused by mid-stream cancellation
                pass
            print "F-DONE"

        print "COMPUTE", xSpan, ySpan
        crMin, crMax, Nx = xSpan
        ciMin, ciMax, Ny = ySpan
        # We have at most 5 calls in the process queue for each worker
        # servicing them, to allow midstream canceling and interleave
        # parallel computation requests.
        ds = defer.DeferredSemaphore(5*self.N_processes)
        p = OrderedItemProducer()
        print "C1"
        yield p.start(f)
        print "C2"
        # "The pickle module keeps track of the objects it has already
        # serialized, so that later references to the same object won't be
        # serialized again." --Python docs
        for k, ci in enumerate(np.linspace(ciMax, ciMin, Ny)):
            # "Wait" for the number of pending calls to fall back to
            # the limit
            print "COMPUTE-{:d}".format(k)
            yield ds.acquire()
            # Make sure the render hasn't been canceled
            if getattr(dCancel, 'called', False):
                break
            # Call one of my processes to get each row of values,
            # starting from the top
            d = p.produceItem(
                self.q.call, self.mv, crMin, crMax, Nx, ci,
                series='process')
            d.addCallback(lambda _: ds.release())
        yield p.stop()
        defer.returnValue(Nx*(k+1))

    def showStats(self, callInfo):
        """
        Displays stats about the run on stdout
        """
        def gotStats(stats):
            x = np.asarray(stats)
            workerTime, processTime = [np.sum(x[:,k]) for k in (0,1)]
            print "Run stats, with {:d} parallel ".format(self.N_processes) +\
                "processes running {:d} calls\n{}".format(len(stats), "-"*70)
            print "Process:\t{:7.2f} seconds, {:0.1f}% of main".format(
                processTime, 100*processTime/totalTime)
            print "Worker:\t\t{:7.2f} seconds, {:0.1f}% of main".format(
                workerTime, 100*workerTime/totalTime)
            print "Total on main:\t{:7.2f} seconds".format(totalTime)
            diffs = 1000*(x[:,0] - x[:,1])
            mean = np.mean(diffs)
            print "Mean worker-to-process overhead (ms/call): {:0.7f}".format(
                mean)
        totalTime = callInfo[0]
        print "Computed {:d} pixels in {:1.1f} seconds.".format(
            callInfo[1], totalTime)
        return self.q.stats().addCallback(gotStats)


def run(*args, **kw):
    """
    Call with::

      [-N, <values>,] [-s, <steepness>,] [-o, <imageFile>,]
      Nx, cr, ci, crPM[, ciPM]

    Writes PNG image to stdout unless -o is set, then saves it to
    C{imageFile}. In that case, prints some stats about the
    multiprocessing computation to stdout.

    @keyword N_values: Integer number of possible values for
      Mandelbrot points during iteration. Can set with the C{-N
      values} arg instead.
    
    @keyword leaveRunning: Set C{True} to let the reactor stay running
      when done.
    """
    @defer.inlineCallbacks
    def reallyRun():
        runner = Runner(N_values, steepness, stats)
        runInfo = yield runner.run(fh, Nx, cr, ci, crPM, ciPM)
        if stats:
            yield runner.showStats(runInfo)
        yield runner.shutdown()
        if not leaveRunning:
            reactor.stop()

    def getOpt(opt, default):
        optCode = "-{}".format(opt)
        if optCode in args:
            k = args.index(optCode)
            args.pop(k)
            optType = type(default)
            return optType(args.pop(k))
        return default
        
    leaveRunning = kw.pop('leaveRunning', False)
    if not args:
        args = sys.argv[1:]
    args = list(args)
    steepness = getOpt('s', 3.0)
    N_values = getOpt('N', 2000)
    fileName = getOpt('o', "")
    if fileName:
        stats = True
        fh = open(fileName, 'w')
    else:
        stats = False
        fh = sys.stdout
    if len(args) < 4:
        print(
            "Usage: [-s steepness] [-N values] [-o imageFile] " +\
            "N cr ci crPM [ciPM]")
        sys.exit(1)
    Nx = int(args[0])
    cr, ci, crPM = [float(x) for x in args[1:4]]
    ciPM = args[4] if len(args) > 4 else crPM
    reactor.callWhenRunning(reallyRun)
    reactor.run()


if __name__ == '__main__':
    run()
