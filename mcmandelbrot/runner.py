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
C{mcm [-s] Nx xMin xMax yMin yMax [N_values] >imagefile.png}

An example of C{AsynQueue} in action. Can be fun to play with if you
have a multicore CPU. You will need the following packages, which you
can get via C{pip install}:

  - C{weave} (part of SciPy)
  - C{numpy} (part of SciPy)
  - C{matplotlib}
  - C{asynqueue} (Duh...)

Here are some command-line args to try. Add a different filename to
each if you want to save a gallery of images to view and zoom in
on. Use PNG format to preserve the most detail.


C{mcm 4000 -2.15 +0.70 -1.20 +1.20 >overview.png}

Overview.


C{mcm 2000 -0.757 -0.743 +0.004 +0.025 >spear.png}

Tip of the upper "spear" separating the main part of the set from the
big secondary bulb at the 9:00 position.


C{mcm 3000 -1.43 -0.63 -0.365 +0.365 >main-bulb.png}

The big secondary bulb.


C{mcm 3000 -1.30 -1.08 +0.192 +0.368 >1100-bulb.png}

The tertiary bulb at the 11:00 position on the big secondary bulb,
with filaments and baby Mandelbrot sets sprouted along their
lengths.


3000 -1.172 -1.152 +0.282 +0.302
================================

The first major intersection along the major filament.


3000 -1.165 -1.160 +0.291 +0.294
================================

Detail of the intersection and the filament branched off to the left
leading to a baby Mandelbrot set that points down and left.


3000 -1.16228 -1.16224 +0.292248 +0.292268
==========================================

An intersection midway along the left filament halfway to the
down-and-left Mandelbrot set


3000 -1.162261 -1.1622590 +0.2922574 +0.2922587
===============================================

Detail of the intersection showing an even smaller Mandelbrot set
there that points up and left.


3000 -1.1622600 -1.1622598 +0.2922580 +0.2922582
================================================

Further detail of the intersection showing the up-and-left Mandelbrot
set with interesting visual features caused by a limited escape cutoff
of 1000 in the value computation loop, L{MandelbrotValuer.__call__}


3000 -1.1622600 -1.1622598 +0.2922580 +0.2922582 <imgFile> 20000
================================================================

Sets I{N_values} to an insanely high cutoff, resulting in something
that looks a lot more boring for the exact same neighborhood in the
complex plane. The image is just another tilted version of the
overview. This goes on forever and ever, until limited by the
numerical precision of the computer.

Running this on my 8-core, 3.5 GHz AMD FX-8320 with C{N_processes=7}
took 18.2 seconds. Total time elapsed running L{MandelbrotValuer} on
the cores was 111.8 seconds, with just under 5 ms of overhead for each
of the 3000 calls. Most of that overhead was idle time spent polling
the interprocess pipe for results and then unpickling the 3000-element
row vectors that arrived over it from the processes.

Running it with C{useThread=True} to keep everything on a single CPU
core took 86.7 seconds, 4.8 times as long. There is some inefficiency
involved with L{process.ProcessQueue}, but it can make a huge
difference for parallel computing tasks with plenty of CPU cores
available.
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
    
    def run(self, fh, Nx, cr, ci, crPM, ciPM):
        """
        Runs my L{compute} method to generate a PNG image of the
        Mandelbrot Set and write it in chunks to the file handle or
        write-capable object I{fh}.

        The image is centered at location I{cr, ci} in the complex
        plane, plus or minus I{crPM} on the real axis and I{ciPM on
        the imaginary axis. If I{ciPM} is not specified, it is the
        same as I{crPM}, resulting in a square image.

        @return: A C{Deferred} that fires with the total elasped time
          for the computation.
        """
        def diff(k):
            return xySpans[k][1] - xySpans[k][0]
        def done(null):
            return time.time() - t0
            
        t0 = time.time()
        xySpans = []
        for center, plusMinus in ((cr, crPM), (ci, ciPM)):
            xySpans.append([center - plusMinus, center + plusMinus])
        xySpans[0].append(Nx)
        xySpans[1].append(int(Nx * diff(1) / diff(0)))
        return self.compute(fh, *xySpans).addCallback(done)
        
    @defer.inlineCallbacks
    def compute(self, fh, xSpan, ySpan):
        """
        Computes the Mandelbrot Set under C{Twisted} and generates a
        pretty image, written as a PNG image to the supplied file
        handle I{fh} one row at a time.

        @return: A C{Deferred} that fires when the image is completely
          written and you can close the file handle.
        """
        def f(rows):
            writer = png.Writer(Nx, Ny, bitdepth=8, compression=9)
            writer.write(fh, rows)

        crMin, crMax, Nx = xSpan
        ciMin, ciMax, Ny = ySpan
        p = OrderedItemProducer()
        yield p.start(f)
        # "The pickle module keeps track of the objects it has already
        # serialized, so that later references to the same object t be
        # serialized again." --Python docs
        for k, ci in enumerate(np.linspace(ciMax, ciMin, Ny)):
            # Call one of my processes to get each row of values,
            # starting from the top
            p.produceItem(
                self.q.call, self.mv, crMin, crMax, Nx, ci,
                series='process')
        yield p.stop()

    def showStats(self, totalTime):
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
        print "Total time: {:1.1f} seconds.".format(totalTime)
        return self.q.stats().addCallback(gotStats)


def run(*args, **kw):
    """
    Call with
    C{[-N values] [-s steepness] [-o imageFile] Nx, cr, ci, crPM[, ciPM]}

    Writes PNG image to stdout unless -o is set, then saves it to
    C{imageFile}. In that case, prints some stats about the
    multiprocessing computation to stdout.

    @keyword N_values: Integer number of possible values for
      Mandelbrot points during iteration. Can set with the C{-N
      values} arg instead.
    
    @keyword leaveRunning: Set C{True} to let the reactor stay running
      when done.
    """
    def reallyRun():
        runner = Runner(N_values, steepness, stats)
        d = runner.run(fh, Nx, cr, ci, crPM, ciPM)
        if stats:
            d.addCallback(runner.showStats)
        if not leaveRunning:
            d.addCallback(lambda _: reactor.stop())
        return d

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
