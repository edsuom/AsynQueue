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


class MandelbrotSiteResource(resource.Resource):
    defaultPosition = {
        'cr':   "-0.630",
        'ci':   "+0.000",
        'crpm': "1.40" }
    defaultTitle = "Interactive Mandelbrot Set: "+\
                   "Driven by Twisted and AsynQueue"
    
    def __init__(self):
        resource.Resource.__init__(self)
        self.vr = vroot.HTML_VRoot()
        self.vr.title = self.defaultTitle

    def shutdown(self):
        return self.imageResource.imager.shutdown()

    def imgURL(self, dct):
        parts = ["{}={}".format(name, value)
                 for name, value in dct.iteritems()]
        return "/image.png?{}".format('&'.join(parts))
        
    def render_GET(self, request):
        with self.vr as v:
            heading = v.nc('div', 'heading')
            #--------------------------------------------------------
            v.nc('div', 'left')
            v.ngc('p', 'bigger').text = "Interactive Mandelbrot Set"
            v.ns('div', 'right')
            v.nc('p', 'smaller')
            v.text("Powered by ")
            v.nc('a')
            v.text("Twisted")
            v.set('href', "http://twistedmatrix.com")
            v.tail(" and ")
            v.ns('a')
            v.text("AsynQueue")
            v.set('href', "http://edsuom.com/AsynQueue")
            v.tail(".")
            #--------------------------------------------------------
            v.rp()
            v.nc('div', 'clear').text = " "
            formDiv = v.ns('div', 'form')
            v.nc('form')
            v.set('name', "position")
            v.set('action', "javascript:updateImage()")
            v.textX("Real:&nbsp;")
            for inputName, tailText in v.nci(
                    (('cr', "+/-"), ('crpm', "Imaginary:"), ('ci', "")),
                    'input'):
                v.set('type', "text")
                v.set('id', inputName)
                v.set('value', self.defaultPosition[inputName])
                #v.set('pattern', r'[\+\-]?[0-9]+(\.[0-9]+|)')
                v.tailX("&nbsp;&nbsp;{}&nbsp;".format(tailText))
            v.nc('input')
            v.set('type', "submit")
            v.set('value', "Reload")
            v.nc('button', formDiv)
            v.set('type', "button")
            v.set('onclick', "zoomOut()")
            v.text("Zoom Out")
            #--------------------------------------------------------
            v.rp()
            v.nc('div', 'image')
            v.set('id', 'image')
            v.nc('img', 'mandelbrot')
            v.set('id', 'mandelbrot')
            v.set('src', self.imgURL(self.defaultPosition))
            v.set('onclick', "zoomIn(event)")
            v.set('onmousemove', "hover(event)")
            v.ns('div', 'coords')
            v.set('id', 'hover')
        return self.vr()


class MandelbrotImageResource(resource.Resource):
    isLeaf = True
    
    def __init__(self):
        resource.Resource.__init__(self)
        self.imager = image.Imager(verbose=VERBOSE)
        
    def render_GET(self, request):
        request.setHeader("Content-Type", 'image/png')
        self.imager.renderImage(request)
        return server.NOT_DONE_YET


class MandelbrotSite(server.Site):
    def __init__(self):
        rootResource = MandelbrotSiteResource()
        imageResource = MandelbrotImageResource()
        rootResource.putChild('image.png', imageResource)
        rootResource.putChild('', rootResource)
        server.Site.__init__(self, rootResource)
    
    def stopFactory(self):
        super(MandelbrotSite, self).stopFactory()
        return self.resource.shutdown()


if '/twistd' in sys.argv[0]:
    site = MandelbrotSite()
    application = service.Application("Mandelbrot Set image server")
    internet.TCPServer(PORT, site).setServiceParent(application)
