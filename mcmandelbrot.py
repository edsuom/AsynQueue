#!/usr/bin/env python
#
# mcmandelbrot
#
# An example module/program for AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker interface.
#
# Copyright (C) 2006-2007, 2015 by Edwin A. Suominen,
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
mcmandelbrot.py [-s] Nx xMin xMax yMin yMax [filePath [N_values]]

An example of C{AsynQueue} in action. Can be fun to play with if you
have a multicore CPU. You will need the following packages, which you
can get via C{pip install}:

  - C{weave} (part of SciPy)
  - C{numpy} (part of SciPy)
  - C{matplotlib}
  - C{asynqueue} (Duh...)

Here are some command-line args to try. Add a different filename to
each if you want to save a gallery of images to view and zoom in
on. Use PNG format.


4000 -2.15 +0.70 -1.20 +1.20
============================
Overview.


2000 -0.757 -0.743 +0.004 +0.025
================================

Tip of the upper "spear" separating the main part of the set from the
big secondary bulb at the 9:00 position.


3000 -1.43 -0.63 -0.365 +0.365
==============================

The big secondary bulb.


3000 -1.30 -1.08 +0.192 +0.368
==============================

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
of 512 in the value computation loop, L{MandelbrotValuer.__call__}


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

import sys, time

import weave
import numpy as np
import matplotlib.pyplot as plt

from twisted.internet import defer, reactor

import asynqueue


class MandelbrotValuer(object):
    """
    Returns the values (number of iterations to escape, if at all,
    inverted) of the Mandelbrot set at point cr + i*ci in the complex
    plane, for a range of real values with a constant imaginary component.

    C code adapted from Ilan Schnell's C{iterations} function at::
    
      https://svn.enthought.com/svn/enthought/Mayavi/
        branches/3.0.4/examples/mayavi/mandelbrot.py}

    The values are inverted, i.e., subtracted from the maximum value, so
    that no-escape points (technically, the only points actually in
    the Mandelbrot Set) have zero value and points that escape
    immediately have the maximum value. This allows simple mapping to
    the classic image with a black area in the middle.
    """
    code = """
    int j, k;
    double zr, zi, zr2, zi2;
    for (j=0; j<Nx[0]; j++) {
        k = 1;
        zr = X1(j);
        zi = ci;
        while ( k < kmax ) {
            zr2 = zr * zr;
            zi2 = zi * zi;
            if ( zr2+zi2 > 16.0 ) break;
            zi = 2.0 * zr * zi + ci;
            zr = zr2 - zi2 + X1(j);
            k++;
        }
        // Invert so that the fastest escape has the max value
        k = kmax - k;
        // Scale to range 0.0-1.0 and, since this value of c isn't needed
        // anymore, put the result in the array item
        X1(j) = (double)(k) / kmax;
    }
    """
    vars = ['x', 'ci', 'kmax']

    def __init__(self, N_values):
        self.N_values = N_values
    
    def __call__(self, crMin, crMax, N, ci):
        """
        Computes values for I{N} points along the real (horizontal) axis
        from I{crMin} to I{crMax}, with the constant imaginary
        component I{ci}.

        @return: A 1-D C{NumPy} array of length I{N} containing the
          values as normalized 16-bit floats in the range
          0.0-1.0. It's a small datatype but entirely adequate.
        
        """
        x = np.linspace(crMin, crMax, N, dtype=np.float64)
        kmax = self.N_values - 1
        weave.inline(self.code, self.vars)
        return x.astype(np.float16)


class Runner(object):
    """
    I run a multi-process Mandelbrot Set computation operation.

    @cvar N_processes: The number of processes to use, disregarded if
      I{useThread} is set C{True} in my constructor.
    """
    N_processes = 7

    def __init__(
            self, Nx, xMin, xMax, yMin, yMax, useThread=False, stats=False):
        self.xSpan = (xMin, xMax, Nx)
        Ny = int(Nx * (yMax - yMin) / (xMax - xMin))
        self.ySpan = (yMin, yMax, Ny)
        if useThread:
            self.stats = False
            self.q = asynqueue.TaskQueue()
            self.q.attachWorker(asynqueue.ThreadWorker())
        else:
            self.stats = stats
            self.q = asynqueue.ProcessQueue(self.N_processes, callStats=stats)

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
          is 512.
        
        """
        @defer.inlineCallbacks
        def reallyRun():
            t0 = time.time()
            z = yield self.compute(N_values)
            totalTime = time.time() - t0
            if z is None:
                reactor.stop()
                sys.exit(1)
            self.plot(z, imgFilePath)
            print "Computed {:d} values in {:1.1f} seconds.".format(
                z.size, totalTime)
            if self.stats:
                stats = yield self.q.stats()
                self.showStats(totalTime, stats)
            reactor.stop()

        imgFilePath = args[0] if args else None
        N_values = int(args[1]) if len(args) > 1 else 512
        reactor.callWhenRunning(reallyRun)
        reactor.run()
        
    @defer.inlineCallbacks
    def compute(self, N_values):
        """
        Computes the Mandelbrot Set for my complex region of interest and
        returns a C{Deferred} that fires with a 2D C{NumPy} array of
        normalized values.
        """
        def gotResult(row, k):
            if isinstance(row, str):
                print "ERROR: ", row
            else:
                z[k,:] = row

        mv = MandelbrotValuer(N_values)
        crMin, crMax, Nx = self.xSpan
        dt = asynqueue.DeferredTracker()
        z = np.zeros((self.ySpan[2], Nx), dtype=np.float16)
        for k, ci in self.frange(*self.ySpan):
            # Call one of my processes to get a row of values
            d = self.q.call(mv, crMin, crMax, Nx, ci)
            d.addCallback(gotResult, k)
            dt.put(d)
        yield dt.deferToAll()
        defer.returnValue(z)

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

    def plot(self, z, imgFilePath=None):
        """
        Color-maps values in the 2D array I{z} and renders a pseudocolor plot.
        """
        if imgFilePath:
            plt.imsave(imgFilePath, z, origin='lower')
            return
        fig = plt.figure()
        extent = [
            self.xSpan[0], self.xSpan[1],
            self.ySpan[0], self.ySpan[1]]
        plt.imshow(z, origin='lower', extent=extent, aspect='equal')
        plt.colorbar()
        plt.show()

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
        print "Mean worker-to-process overhead (ms per call): {:0.7f}".format(
            mean)


def run(*args, **kw):
    """
    Call with [-s,] Nx, xMin, xMax, yMin, yMax[, filePath[, N_values]]

    @keyword callStats: Set C{True} to print stats about calls.
    """
    if not args:
        args = sys.argv[1:]
    if '-s' in args:
        kw['stats'] = True
        args.remove('-s')
    if len(args) < 5:
        print "Arguments: N rMin rMax iMin iMax [imageFile]"
        sys.exit(1)
    newArgs = [int(args[0])]
    newArgs.extend([float(x) for x in args[1:5]])
    Runner(*newArgs, **kw).run(*args[5:])


if __name__ == '__main__':
    run()
