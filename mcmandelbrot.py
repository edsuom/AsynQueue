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

import weave
from PIL import Image

from twisted.internet import reactor

import asynqueue



class MandelbrotValuer(object):
    """
    Returns the value (number of iterations to escape, if at all,
    inverted) of the Mandelbrot set at point cr + i*ci in the complex
    plane.

    C code adapted from Ilan Schnell's C{iterations} function at::
    
      https://svn.enthought.com/svn/enthought/Mayavi/
        branches/3.0.4/examples/mayavi/mandelbrot.py}

    The value is inverted, i.e., subtracted from the maximum value, so
    that no-escape points (technically, the only points actually in
    the Mandelbrot Set) have zero value and points that escape
    immediately have the maximum value. This allows simple mapping to
    the classic image with a black area in the middle.
    """
    maxValue = 255
    
    code = """
    int k = 1;
    double zr=cr, zi=ci, zr2, zi2;
    while ( k < kmax ) {
        zr2 = zr * zr;
        zi2 = zi * zi;
        if ( zr2+zi2 > 16.0 ) break;
        zi = 2.0 * zr * zi + ci;
        zr = zr2 - zi2 + cr;
        k++;
    }
    k = kmax - k;
    return_val = k;
    """
    vars = ['cr', 'ci', 'kmax']

    def __call__(self, cr, ci):
        kmax = self.maxValue
        return weave.inline(self.code, self.vars)



class Runner(object):
    """
    """
    N_processes = 8
    rgThreshold = 200
    
    def __init__(self, xMin, xMax, yMin, yMax, N=2):
        self.xSpan = (xMin, xMax, N)
        self.ySpan = (yMin, yMax, N)
        self.q = asynqueue.ProcessQueue(self.N_processes)

    def frange(self, minVal, maxVal, N):
        val = minVal
        step = (maxVal - minVal) / N
        for k in xrange(N):
            yield val
            val += step

    def mapValueToRGB(self, value):
        if value > self.rgThreshold:
            return (0, value, 0)
        return (value, 0, 0)
            
    def run(self, imagePath):
        """
        """
        def gotValue(value):
            im[cr, ci] = self.mapValueToRGB(value)

        def done(null):
            im.save(imagePath)
            #reactor.stop()
            
        mv = MandelbrotValuer()
        dt = asynqueue.DeferredTracker()
        im = Image.new("RGB", (self.xSpan[2], self.ySpan[2]))
        for cr in self.frange(*self.xSpan):
            for ci in self.frange(*self.ySpan):
                d = self.q.call(mv, cr, ci)
                d.addCallback(gotValue)
                dt.put(d)
        return dt.deferToAll().addCallback(done)


def run(*args):
    """
    Call with xMin, xMax, yMin, yMax, filePath
    """
    if not args:
        import sys
        args = []
        for k in (2,3):
            for x in sys.argv[k].split('-'):
                args.append(float(x.strip()))
        args.append(sys.argv[4])
    runner = Runner(*args[:4])
    reactor.callWhenRunning(runner.run, args[4])
    reactor.run()




    
