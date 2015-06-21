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
A simple PyQt4 GUI for mcMandelbrot.

B{TODO}

"""

from PyQt4 import QtGUI

class MainWindow(QtGui.QMainWindow):
    """
    I act as the main window for the QApplication object.
    """
    def __init__(self, Nx, cr, ci, crPM):
        """
        Instantiates me with a left panel containing form fields and user
        control buttons and a right panel with the image.
        """
        QtGui.QMainWindow.__init__(self)
        reactor.callWhenRunning(self.show)
