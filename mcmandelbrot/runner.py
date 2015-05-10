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
    
    def __init__(self, q, fh, Nx, Ny):
        self.writeConfig = q, fh, Nx, Ny
        self.rowCount = 0
        self.rowBuffer = {}
        self.consumerator = Consumerator(self)
        self.produce = True
        self.d = defer.Deferred()

    def runWriter(self):
        """
        Runs the blocking PNG writer in the queue via a thread worker.
        """
        def func():
            writer = png.Writer(Nx, Ny, bitdepth=8, compression=9)
            writer.write(fh, self.consumerator)
        def done(result):
            if isinstance(result, str):
                print str
        q, fh, Nx, Ny = self.writeConfig
        return q.call(func, series='thread').addCallback(done)
        
    def handleRow(self, row, k):
        """
        Handles a I{row} (unsigned byte array of RGB triples) from one of
        the processes at row index I{k}.
        """
        if k == self.rowCount and self.produce:
            self.consumerator.write(row)
            return
        if self.produce is None:
            return
        self.rowBuffer[k] = row

    def done(self):
        return self.d.addCallback(lambda _: self.consumerator.stop())
    
    def stopProducing(self):
        self.produce = None

    def pauseProducing(self):
        self.produce = False

    def resumeProducing(self):
        self.rowCount += 1
        if self.rowCount == self.writeConfig[-1]:
            # We're done
            self.d.callback(None)
            return
        # More to write
        if self.rowCount in self.rowBuffer:
            row = self.rowBuffer.pop(self.rowCount)
            # This results in another call here, with nesting
            self.consumerator.write(row)
        # More to produce
        self.produce = True
        

class Runner(object):
    """
    I run a multi-process Mandelbrot Set computation operation.

    @cvar N_processes: The number of processes to use, disregarded if
      I{useThread} is set C{True} in my constructor.
    """
    power = 5.0
    N_values = 1000
    N_processes = 7
    N_threads = 1

    def __init__(self, N_values=None, stats=False):
        if N_values is None:
            N_values = self.N_values
        self.q = asynqueue.ProcessQueue(self.N_processes, callStats=stats)
        for k in xrange(self.N_threads):
            self.q.attachWorker(asynqueue.ThreadWorker())
        self.mv = MandelbrotValuer(N_values, self.power)

    def run(self, fh, Nx, xMin, xMax, yMin, yMax):
        """
        Runs my L{compute} method to generate a PNG image of the
        Mandelbrot Set and write it to the file handle or
        write-capable object I{fh}.

        @return: A C{Deferred} that fires with the total elasped time
          for the computation.
        """
        def done(null):
            return time.time() - t0
            
        t0 = time.time()
        xSpan = (xMin, xMax, Nx)
        Ny = int(Nx * (yMax - yMin) / (xMax - xMin))
        ySpan = (yMin, yMax, Ny)
        return self.compute(fh, xSpan, ySpan).addCallback(done)

    @defer.inlineCallbacks
    def compute(self, fh, xSpan, ySpan):
        """
        Computes the Mandelbrot Set under C{Twisted} and generates a
        pretty image, written as a PNG image to the supplied file
        handle I{fh} one row at a time.

        @return: A C{Deferred} that fires when the image is completely
          written and you can close the file handle.
        """
        ib = ImageBuilder(self.q, fh, xSpan[2], ySpan[2])
        dWriter = ib.runWriter()
        crMin, crMax, Nx = xSpan
        # "The pickle module keeps track of the objects it has already
        # serialized, so that later references to the same object t be
        # serialized again." --Python docs
        dList = []
        for k, ci in self.frange(*ySpan):
            # Call one of my processes to get a row of values
            d = self.q.call(self.mv, crMin, crMax, Nx, ci, series='process')
            d.addCallback(ib.handleRow, k)
            dList.append(d)
        yield defer.DeferredList(dList)
        yield ib.done()
        yield dWriter

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

    def showStats(self, N, totalTime):
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

        print "Computed {:d} values in {:1.1f} seconds.".format(
                N, totalTime)
        return self.q.stats().addCallback(gotStats)


def run(*args, **kw):
    """
    Call with C{[-N values] [-o imageFile] Nx, xMin, xMax, yMin, yMax}

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
        runner = Runner(N_values, stats)
        d = runner.run(fh, Nx, xMin, xMax, yMin, yMax)
        if not leaveRunning:
            d.addCallback(lambda _: reactor.stop())
        return d

    leaveRunning = kw.pop('leaveRunning', False)
    if not args:
        args = sys.argv[1:]
    args = list(args)
    # -N values
    if '-N' in args:
        k = args.index('-N')
        args.pop(k)
        N_values = args.pop(k)
    else:
        N_values = kw.get('N_values', None)
    # -o <fileName>
    if '-o' in args:
        k = args.index('-o')
        stats = True
        args.pop(k)
        fh = open(args.pop(k), 'w')
    else:
        stats = False
        fh = sys.stdout
    if len(args) < 5:
        print "Usage: [-N values] [-o imageFile] N rMin rMax iMin iMax"
        sys.exit(1)
    Nx = int(args[0])
    xMin, xMax, yMin, yMax = [float(x) for x in args[1:5]]
    reactor.callWhenRunning(reallyRun)
    reactor.run()


if __name__ == '__main__':
    run()
