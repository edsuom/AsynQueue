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
Colormapping, which is a tricky business when visualizing fractals
that you can zoom in on.
"""

import os.path
from array import array

import numpy as np
 

class ColorMapper(object):
    """
    I map floating-point values in the range 0.0 to 1.0 to RGB byte
    triplets.

    @cvar fileName: A file with a colormap of RGB triplets, one for
      each of many linearly increasing values to be mapped, in CSV
      format.
    """
    N_colors = 6000
    dither = 0.05 / N_colors

    def __init__(self):
        self.rgb = self.loadMap(self.N_colors)
        self.jMax = len(self.rgb) - 1

    def loadMap(self, N):
        """
        Returns an RGB colormap of dimensions C{Nx3} that transitions from
        black to red, then red to orange, then orange to white.
        """
        ranges = [
            [0.000, 2.8/3],  # Red component ranges
            [2.5/3, 3.0/3],  # Green component ranges
            [2.7/3, 1.000],  # Blue component ranges
        ]
        limits = [255, 220, 180]
        bluecycle = [20, 120]
        rgb = self._rangeMap(N, ranges, limits)
        x = bluecycle[0]*np.sin(np.linspace(0, bluecycle[1]*2*3.141591, N))
        rgb[:,2] += x.astype(np.uint8) + bluecycle[0]
        return rgb

    def _rangeMap(self, N, ranges, limits):
        rgb = np.zeros((N, 3), dtype=np.uint8)
        kt = np.rint(N*np.array(ranges)).astype(int)
        # Range #1: Increase red
        rgb[0:kt[0,1],0] = np.linspace(0, limits[0], kt[0,1])
        # Range #2: Max red, increase green
        rgb[kt[0,1]:,0] = limits[0]
        rgb[kt[1,0]:kt[1,1],1] = np.linspace(0, limits[1], kt[1,1]-kt[1,0])
        # Range #3: Max red and green, increase blue
        rgb[kt[1,1]:,1] = limits[1]
        rgb[kt[2,0]:,2] = np.linspace(0, limits[2], kt[2,1]-kt[2,0])
        return rgb
    
    def __call__(self, x):
        result = array('B')
        np.clip(x + (self.dither * np.random.randn(len(x))), 0, 1.0, x)
        np.rint(self.jMax * x, x)
        for j in x.astype(np.uint16):
            result.extend(self.rgb[j,:])
        return result

    
