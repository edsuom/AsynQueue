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

import sys

import weave
import numpy as np
from PIL import Image

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
    N_values = 512
    colorMap = [
        (0.70,   0, 180), # Red
        (0.90,  80, 240), # Green
        (1.00, 128, 255)  # Blue
    ]
    
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
        // This value of c isn't needed any more, so re-use the array item
        X1(j) = (double)(kmax - k);
    }
    """
    vars = ['x', 'ci', 'kmax']

    def __init__(self):
        prevThreshold = 0
        self.thresholds = [0]
        self.coefficients = []
        for j, row in enumerate(self.colorMap):
            threshold = int(self.N_values * row[0])
            self.thresholds.append(threshold)
            coefficient = float(row[2] - row[1]) / (threshold - prevThreshold)
            self.coefficients.append(coefficient)
            prevThreshold = threshold
        self.rgbLast = (None, None)
    
    def __call__(self, crMin, crMax, N, ci):
        """
        Computes values for I{N} points along the real (horizontal) axis
        from I{crMin} to I{crMax}, with the constant imaginary
        component I{ci}. Maps the values to RGB byte triplets and
        returns a L{bytearray} of length 3N.
        """
        x = np.linspace(crMin, crMax, N, dtype='float64')
        kmax = self.N_values - 1
        weave.inline(self.code, self.vars)
        return self.valuesToRGB(x)
        
    def valuesToRGB(self, x):
        """
        Maps the values in the supplied array I{x} into a bytearray of 3x
        length with the RGB pixel color.
        """
        def scale(value, k):
            diff = value - self.thresholds[k]
            result = int(self.coefficients[k] * diff) + self.colorMap[k][1]
            if result < 0:
                return 0
            if result > 255:
                return 255
            return result
        
        def mapValue(value):
            # There are a lot of repeated values, so we cache the last
            # one
            if self.rgbLast[0] == value:
                # Yes, another repeat. Use it.
                return self.rgbLast[1]
            if value > self.thresholds[0]:
                if value > self.thresholds[1]:
                    # Blue
                    rgb = (0, 0, scale(value, 2))
                else:
                    # Green
                    rgb = (0, scale(value, 1), 0)
            else:
                # Red
                rgb = (scale(value, 0), 0, 0)
            # Cache the input value and mapped value
            self.rgbLast = (value, rgb)
            return rgb

        N = x.shape[0]
        k012 = (0,1,2)
        result = bytearray(3*N)
        for j in xrange(N):
            rgb = mapValue(x[j])
            for k in k012:
                result[3*j+k] = rgb[k]
        return result


class Runner(object):
    """
    """
    N_processes = 4

    def __init__(self, xMin, xMax, Nx, yMin, yMax, Ny):
        self.xSpan = (xMin, xMax, Nx)
        self.ySpan = (yMin, yMax, Ny)
        #self.q = asynqueue.TaskQueue()
        #self.q.attachWorker(asynqueue.ThreadWorker())
        self.q = asynqueue.ProcessQueue(self.N_processes)

    def frange(self, minVal, maxVal, N):
        val = float(minVal)
        step = (float(maxVal) - val) / N
        for k in xrange(N):
            yield k, val
            val += step

    def oops(self, failureObj):
        failureObj.printTraceback()
        reactor.stop()
            
    @defer.inlineCallbacks
    def run(self, imagePath):
        """
        Computes the Mandelbrot Set for my complex region of interest and
        generates an RGB image, saving it to the specified
        I{imagePath}.
        """
        def gotResult(rgbs, k):
            if not self.isRunning:
                return
            if isinstance(rgbs, str):
                print str
                self.isRunning = False
                return
            for j in xrange(N):
                im.im.putpixel((j, k), tuple(rgbs[3*j:3*j+3]))

        self.isRunning = True
        mv = MandelbrotValuer()
        crMin, crMax, N = self.xSpan
        dt = asynqueue.DeferredTracker()
        im = Image.new("RGB", (self.xSpan[2], self.ySpan[2]))
        for k, ci in self.frange(*self.ySpan):
            if not self.isRunning:
                break
            d = self.q.call(mv, crMin, crMax, N, ci)
            d.addCallback(gotResult, k)
            dt.put(d)
        yield dt.deferToAll()
        im.save(imagePath)
        reactor.stop()


def run(*args):
    """
    Call with xMin, xMax, yMin, yMax, filePath
    """
    if not args:
        args = [float(x) for x in sys.argv[1:7]]
        args.append(sys.argv[7])
    runner = Runner(*args[:6])
    reactor.callWhenRunning(runner.run, args[6])
    reactor.run()


if __name__ == '__main__':
    run(-2.25, 0.75, 8000, -1.5, +1.5, 8000, 'mcm.png')
    #run()




    
