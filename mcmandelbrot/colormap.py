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
Colormapping with Kenneth Moreland's "Diverging Color Maps for
Scientific Visualization",
U{http://www.sandia.gov/~kmorel/documents/ColorMaps/}.
"""

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
    fileName = "colormap.csv"
    
    def __init__(self):
        self.rgb = np.loadtxt(self.fileName, delimiter=',', dtype=np.uint8)
        self.jMax = len(self) - 1

    def __len__(self):
        return len(self.rgb)
        
    def __call__(self, x):
        result = array('B')
        np.rint(self.jMax * x, x)
        for j in x.astype(np.uint16):
            result.extend(self.rgb[j,:])
        return result

    
