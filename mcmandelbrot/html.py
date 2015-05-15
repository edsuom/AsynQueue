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
A Twisted web C{Resource} that serves clickable, zoomable
Mandelbrot Set images.
"""

import sys

from twisted.application import internet, service
from twisted.internet import defer
from twisted.web import server, resource

from mcmandelbrot import vroot, image


PORT = 8080
VERBOSE = True


class MandelbrotImageResource(resource.Resource):
    isLeaf = True
    
    def __init__(self):
        self.imager = image.Imager(verbose=VERBOSE)
        
    def render_GET(self, request):
        request.setHeader("Content-Type", 'image/png')
        self.imager.renderImage(request)
        return server.NOT_DONE_YET


class MandelbrotSite(server.Site):
    def stopFactory(self):
        super(MandelbrotSite, self).stopFactory()
        return self.resource.imager.shutdown()


if '/twistd' in sys.argv[0]:
    application = service.Application("Mandelbrot Set image server")
    resource = MandelbrotImageResource()
    site = server.Site(resource)
    internet.TCPServer(PORT, site).setServiceParent(application)
