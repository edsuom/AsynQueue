# AsynQueue:
# Asynchronous task queueing based on the Twisted framework, with task
# prioritization and a powerful worker/manager interface.
#
# Copyright (C) 2006-2007, 2015 by Edwin A. Suominen,
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
Unit tests for mcmandelbrot.valuer
"""

from contextlib import contextmanager
from collections import namedtuple

from scipy import weave
import numpy as np

from twisted.internet import defer

from mcmandelbrot import valuer
from mcmandelbrot.test.testbase import TestCase


class TestMandelbrotValuer(TestCase):
    verbose = True

    class Yielded:
        pass
    msgProto = "With {:d} max iterations at {:d} points "+\
               "from ({:+f}, {:+f}) to ({:+f}, {:+f}):\n{}\n"
    
    def setUp(self):
        self.mv = valuer.MandelbrotValuer(1000)

    @contextmanager
    def _crpmToXQD(self, cr, ci, crpm, N):
        crMin = cr - crpm
        crMax = cr + crpm
        Y = self.Yielded()
        Y.x = np.linspace(crMin, crMax, N, dtype=np.float64)
        Y.qd = 0.25 * (crMax - crMin) / N
        yield Y
        self.msg(
            self.msgProto,
            self.mv.N_values, N, crMin, ci, crMax, ci, Y.z)
        
    def _eval_point(self, N, x, ci):
        code = """
        #define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
        int j, m;
        for (j=0; j<Nx[0]; j++) {
            Z1(j) = eval_point(j, km, X1(j), ci);
        }
        """
        km = self.mv.N_values - 1
        self.assertEqual(km, 999)
        z = np.zeros(N, dtype=np.int)
        weave.inline(
            code, ['x', 'z', 'ci', 'km'],
            customize=self.mv.infoObj, support_code=self.mv.support_code)
        return z

    def test_eval_point(self):
        N    = 10
        cr   = -0.5332179372197309
        ci   = +0.535867264573991
        crpm = 0.0022400000000000002
        #--------------------------------------
        with self._crpmToXQD(cr, ci, crpm, N) as Y:
            Y.z = self._eval_point(N, Y.x, ci)
        self.assertEqual(min(Y.z), 75)
        self.assertEqual(max(Y.z), 999)
        
    def test_run_computeValues(self):
        N    = 10
        cr   = -0.5332179372197309
        ci   = +0.535867264573991
        crpm = 0.0022400000000000002
        #--------------------------------------
        with self._crpmToXQD(cr, ci, crpm, N) as Y:
            Y.z = self.mv.computeValues(N, Y.x, ci, Y.qd)
        self.assertEqual(min(Y.z), 232)
        self.assertEqual(max(Y.z), 4995)

        
