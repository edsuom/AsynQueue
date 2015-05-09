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
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


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
import weave
from weave.base_info import custom_info
import numpy as np
import matplotlib.cm as cm
import matplotlib.pyplot as plt

from zope.interface import implements
from twisted.internet import defer, reactor
from twisted.internet.interfaces import IPushProducer

import asynqueue
from asynqueue.threads import Consumerator

from valuer import MandelbrotValuer


class ImageBuilder(object):
    """
    I build a PNG image from the computations, running the blocking
    C{png.Writer.write} call in a thread via my own worker (and
    series) in the TaskQueue.
    """
    implements(IPushProducer)
    
    def __init__(self, q):
        self.q = q
        self.q.attachWorker(asynqueue.ThreadWorker())
        self.rowCount = 0
        self.rowBuffer = {}
        self.consumerator = Consumerator(self)
        self.produce = True

    def runWriter(self, fh, Nx, Ny):
        def func():
            writer = png.Writer(Nx, Ny, bitdepth=8, compression=9)
            writer.write(fh, self.consumerator)
        return self.q.call(func, series='thread')
        
    def handleRow(self, row, k):
        """
        Handles a I{row} (unsigned byte array of RGB triples) from one of
        the processes at row index I{k}.
        """
        if k == self.rowCount and self.produce:
            self.consumerator.write(row)
            self.rowCount += 1
            return
        if self.produce = None:
            return
        self.rowBuffer[k] = row

    def stopProducing(self):
        self.produce = None

    def pauseProducing(self):
        self.produce = False

    def resumeProducing(self):
        while self.rowCount in self.rowBuffer:
            if not self.produce:
                return
            row = self.rowBuffer.pop(self.rowCount)
            self.consumerator.write(row)
            self.rowCount += 1
        

class Runner(object):
    """
    I run a multi-process Mandelbrot Set computation operation.

    @cvar N_processes: The number of processes to use, disregarded if
      I{useThread} is set C{True} in my constructor.
    """
    power = 5.0
    N_values = 1000
    N_processes = 7

    def __init__(self, Nx, xMin, xMax, yMin, yMax, stats=False):
        self.xSpan = (xMin, xMax, Nx)
        Ny = int(Nx * (yMax - yMin) / (xMax - xMin))
        self.ySpan = (yMin, yMax, Ny)
        self.stats = stats
        self.dt = asynqueue.DeferredTracker()
        self.q = asynqueue.ProcessQueue(self.N_processes, callStats=stats)
        self.ib = ImageBuilder(self.q)

    @defer.inlineCallbacks
    def run(self, *args):
        """
        Computes the Mandelbrot Set under C{Twisted} and generates a
        pretty image.

        @param imgFilePath: The filename for saving the image instead
          of displaying it. PNG format suggested.
        @type imgFilePath: First argument, if any, a string.

        @param N_values: The number of possible discrete values for
          the result, i.e., the number of times to try iterations of
          M{z = z^2 + c} to see if escape is reached and determine
          that C{c} lies outside the Mandelbrot set.
        @type N_values: Second argument, if any, an integer. Default
          is 1000.
        
        """
        imgFilePath = args[0] if args else None
        N_values = int(args[1]) if len(args) > 1 else self.N_values
        self.dt.put(self.rm.init(self.xSpan[2], self.ySpan[2], N_values))
        t0 = time.time()
        N = yield self.compute(N_values)
        totalTime = time.time() - t0
        print "Computed {:d} values in {:1.1f} seconds.".format(
            N, totalTime)
        if self.stats:
            stats = yield self.q.stats()
            self.showStats(totalTime, stats)
        
    @defer.inlineCallbacks
    def compute(self, N_values):
        """
        Computes the Mandelbrot Set for my complex region of interest and
        returns a C{Deferred} that fires with a 2D C{NumPy} array of
        normalized values.
        """
        yield self.dt.deferToAll()
        crMin, crMax, Nx = self.xSpan
        mv = MandelbrotValuer(N_values)
        for k, ci in self.frange(*self.ySpan):
            # Call one of my processes to get a row of values
            d = self.q.call(mv, crMin, crMax, Nx, ci, series='process')
            d.addCallback(self.rm.handleRow, k)
            self.dt.put(d)
        yield self.dt.deferToAll()
        defer.returnValue(len(self.rm))

    def frange(self, minVal, maxVal, N):
        """
        Iterates over a range of I{N} evenly spaced floats from I{minVal}
        to I{maxVal}, yielding the iteration index and the value.
        """
        val = float(minVal)
        step = (float(maxVal) - val) / N
        for k in xrange(N):
            yield k, val
            val += step

    def showStats(self, totalTime, stats):
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


def run(*args, **kw):
    """
    Call with [-s,] Nx, xMin, xMax, yMin, yMax[, filePath[, N_values]]

    @keyword callStats: Set C{True} to print stats about calls.

    @keyword stopWhenDone: Set C{True} to stop the reactor when done.
    """
    def reallyRun():
        runner = Runner(*newArgs, **kw)
        d = runner.run(*args[5:])
        if stopWhenDone:
            d.addCallback(lambda _: reactor.stop())
        return d
    
    if not args:
        args = sys.argv[1:]
    if '-s' in args:
        kw['stats'] = True
        args.remove('-s')
    if len(args) < 5:
        print "Arguments: N rMin rMax iMin iMax [imageFile [N_values]]"
        sys.exit(1)
    newArgs = [int(args[0])]
    newArgs.extend([float(x) for x in args[1:5]])
    stopWhenDone = kw.pop('stopWhenDone', False)
    reactor.callWhenRunning(reallyRun)
    reactor.run()


if __name__ == '__main__':
    run(stopWhenDone=True)
