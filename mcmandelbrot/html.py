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


MY_PORT = 8080
VERBOSE = True
HTML_FILE = "mcm.html"

HOWTO = """
Click anywhere in the image to zoom in 5x at that location. Try
exploring the edges of the black&nbsp;&ldquo;lakes.&rdquo;
"""

ABOUT = """
Images genera&shy;ted by the <i>mcmandelbrot</i> demo package
bun&shy;dled with my <a
href="http://edsuom.com/AsynQueue">AsynQueue</a> asyn&shy;chronous
processing pack&shy;age, freely available per the Apache License. A
link back to <a href="http://edsuom.com"><b>edsuom.com</b></a> would
be apprec&shy;iated.
"""

BYLINE = "&mdash;Ed Suominen"


class SiteResource(resource.Resource):
    defaultPosition = {
        'cr':   "-0.630",
        'ci':   "+0.000",
        'crpm': "1.40" }
    defaultTitle = \
        "Interactive Mandelbrot Set: Driven by Twisted and AsynQueue"
    formItems = (
        ("Real:", "cr"   ),
        ("Imag:", "ci"   ),
        ("+/-",   "crpm" ))
    inputSize = 10
    
    def __init__(self):
        resource.Resource.__init__(self)
        self.vr = vroot.HTML_VRoot()
        self.vr.title = self.defaultTitle

    def shutdown(self):
        return self.imageResource.imager.shutdown()

    def render_GET(self, request):
        if not hasattr(self, '_html'):
            # We only need to generate the page HTML once. All that
            # changes are the image and form fields.
            self._html = self.makeHTML()
            if HTML_FILE:
                try:
                    with open(HTML_FILE, 'w') as fh:
                        fh.write(self._html)
                except:
                    pass
        return self._html
        
    def imgURL(self, dct):
        parts = ["{}={}".format(name, value)
                 for name, value in dct.iteritems()]
        return "/image.png?{}".format('&'.join(parts))
        
    def makeHTML(self):
        def heading():
            with v.context():
                v.nc('div', 'heading')
                v.nc('p', 'bigger')
                v.textX("Interactive Mandelbrot&nbsp;Set")
            v.nc('div', 'subheading')
            v.nc('p', 'smaller')
            v.text("Powered by ")
            v.nc('a')
            v.text("Twisted")
            v.set('href', "http://twistedmatrix.com")
            v.tailX("&nbsp;and&nbsp;")
            v.ns('a')
            v.text("AsynQueue")
            v.set('href', "http://edsuom.com/AsynQueue")
            v.tail(".")
        
        with self.vr as v:
            v.nc('div', 'first_part')
            #--------------------------------------------------------
            with v.context():
                heading()
            v.ngc('div', 'clear').text = " "
            with v.context():
                v.nc('div')
                with v.context():
                    v.nc('form')
                    v.nc('div', 'form')
                    v.set('name', "position")
                    v.set('action', "javascript:updateImage()")
                    for label, name in v.nci(
                            self.formItems, 'div', 'form_item'):
                        v.nc('span', 'form_item')
                        v.text(label)
                        v.ns('input', 'position')
                        v.set('type', "text")
                        v.set('size', str(self.inputSize))
                        v.set('id', name)
                        v.set('value', self.defaultPosition[name])
                    v.nc('div', 'form_item')
                    e = v.ngc('input')
                    e.set('type', "submit")
                    e.set('value', "Reload")
                    v.ns('div', 'form_item')
                    e = v.ngc('button')
                    e.set('type', "button")
                    e.set('onclick', "zoomOut()")
                    e.text = "Zoom Out"
                v.nc('div', 'about')
                v.textX(ABOUT)
                v.nc('span', 'byline')
                v.textX(BYLINE)
            v.ns('div', 'second_part')
            #--------------------------------------------------------
            with v.context():
                v.nc('div', 'image')
                v.set('id', 'image')
                v.nc('img', 'mandelbrot')
                v.set('id', 'mandelbrot')
                v.set('src', self.imgURL(self.defaultPosition))
                v.set('onclick', "zoomIn(event)")
                v.set('onmousemove', "hover(event)")
            with v.context():
                v.nc('div', 'footer')
                v.set('id', 'hover')
                v.textX(HOWTO)
        return self.vr()
        

class ImageResource(resource.Resource):
    isLeaf = True
    
    def __init__(self, description=None):
        resource.Resource.__init__(self)
        self.imager = image.Imager(description, verbose=VERBOSE)
        
    def render_GET(self, request):
        request.setHeader("content-disposition", "image.png")
        request.setHeader("content-type", 'image/png')
        self.imager.renderImage(request)
        return server.NOT_DONE_YET


class MandelbrotSite(server.Site):
    def __init__(self):
        rootResource = SiteResource()
        imageResource = ImageResource("tcp:localhost:1978")
        rootResource.putChild('image.png', imageResource)
        rootResource.putChild('', rootResource)
        server.Site.__init__(self, rootResource)
    
    def stopFactory(self):
        super(MandelbrotSite, self).stopFactory()
        return self.resource.shutdown()


if '/twistd' in sys.argv[0]:
    site = MandelbrotSite()
    application = service.Application("Interactive Mandelbrot Set HTTP Server")
    internet.TCPServer(MY_PORT, site).setServiceParent(application)
