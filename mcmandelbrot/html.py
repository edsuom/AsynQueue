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
from twisted.web import server, resource, static, util, http

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
processing pack&shy;age, which is freely available per the Apache
License. A link back to <a
href="http://mcm.edsuom.com"><b>mcm.edsuom.com</b></a> would be
apprec&shy;iated.
"""

BYLINE = " &mdash;Ed Suominen"

MORE_INFO = """
CPU and bandwidth resources for this site were con&shy;tributed by <a
href="http://tellectual.com">Tellectual Press</a>, publisher of my
book <em>Evolving out of Eden</em>.
"""

class ResourceBag(object):
    blankImage = ("blank.jpg", 'image/jpeg')
    children = {}
    
    def __init__(self, descriptions):
        self.children[''] = RootResource(self.blankImage[0])
        with vroot.openPackageFile(self.blankImage[0]) as fh:
            imageData = fh.read()
        self.children[self.blankImage[0]] = static.Data(
            imageData, self.blankImage[1])
        self.children['image.png'] = ImageResource(descriptions)

    def shutdown(self):
        return self.ir.shutdown()

    def putChildren(self, root):
        for path, res in self.children.iteritems():
            root.putChild(path, res)


class RootResource(resource.Resource):
    defaultParams = {
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
    pxHD = 2048
    
    def __init__(self, blankImage):
        self.blankImage = blankImage
        self.vr = self.vRoot()
        resource.Resource.__init__(self)
        
    def render_GET(self, request):
        request.setHeader("content-type", 'text/html')
        kw = {'permalink': request.uri}
        kw.update(self.defaultParams)
        if request.args:
            for key, values in request.args.iteritems():
                kw[key] = http.unquote(values[0])
            kw['onload'] = None
            kw['img'] = self.imageURL(kw)
            kw['hd'] = self.imageURL(kw, N=self.pxHD)
        else:
            kw['onload'] = "updateImage()"
            kw['img'] = self.blankImage
            kw['hd'] = self.imageURL(self.defaultParams, N=self.pxHD)
        return self.vr(**kw)
        
    def imageURL(self, params, **kw):
        """
        Returns a URL for obtaining a Mandelbrot Set image with the
        parameters in the supplied dict I{params}.
        """
        def addPart():
            parts.append("{}={}".format(name, value))
        parts = []
        for name, value in params.iteritems():
            if name in self.defaultParams:
                addPart()
        for name, value in kw.iteritems():
            addPart()
        return "/image.png?{}".format('&'.join(parts))
        
    def vRoot(self):
        """
        Populates my vroot I{vr} with an etree that renders into the HTML
        page.
        """
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

        vr = vroot.VRoot(self.defaultTitle)
        with vr as v:
            v.nc('body')
            v.addToMap('onload', 'onload')
            v.nc('div', 'container')
            v.set('id', 'container')
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
                        v.addToMap(name, 'value')
                        v.set('type', "text")
                        v.set('size', str(self.inputSize))
                        v.set('id', name)
                    v.nc('div', 'form_item')
                    e = v.ngc('input')
                    e.set('type', "submit")
                    e.set('value', "Reload")
                    v.ns('div', 'form_item')
                    e = v.ngc('button')
                    e.set('type', "button")
                    e.set('onclick', "zoomOut()")
                    e.text = "Zoom Out"
                with v.context():
                    v.nc('div', 'about')
                    v.textX(ABOUT)
                    v.nc('span', 'byline')
                    v.textX(BYLINE)
                v.nc('div', 'about large_only')
                v.textX(MORE_INFO)
            v.ns('div', 'second_part')
            #--------------------------------------------------------
            with v.context():
                v.nc('div', 'image')
                v.set('id', 'image')
                with v.context():
                    v.nc('img', 'mandelbrot')
                    v.addToMap('img', 'src')
                    v.set('id', 'mandelbrot')
                    v.set('onclick', "zoomIn(event)")
                    v.set('onmousemove', "hover(event)")
                v.nc('div', 'footer')
                v.nc('div', 'left')
                v.set('id', 'hover')
                v.textX(HOWTO)
                v.ns('div', 'right')
                v.nc('a', 'bold')
                v.text("2048px")
                v.tailX("&nbsp;|&nbsp;")
                v.set('id', 'hd')
                v.addToMap('hd', 'href')
                v.ns('a', 'bold')
                v.text("Permalink")
                v.set('id', 'permalink')
                v.addToMap('permalink', 'href')
            v.ns('div', 'about small_only')
            v.set('id', 'more_info')
            v.textX(MORE_INFO)
            return vr


class ImageResource(resource.Resource):
    isLeaf = True
    
    def __init__(self, descriptions):
        self.imager = image.Imager(descriptions, verbose=VERBOSE)
        resource.Resource.__init__(self)

    def shutdown(self):
        return self.imager.shutdown()
        
    def render_GET(self, request):
        request.setHeader("content-disposition", "image.png")
        request.setHeader("content-type", 'image/png')
        self.imager.renderImage(request)
        return server.NOT_DONE_YET


class MandelbrotSite(server.Site):
    def __init__(self):
        self.rb = ResourceBag([None])
        siteResource = resource.Resource()
        self.rb.putChildren(siteResource)
        server.Site.__init__(self, siteResource)
    
    def stopFactory(self):
        super(MandelbrotSite, self).stopFactory()
        return self.rb.shutdown()


if '/twistd' in sys.argv[0]:
    site = MandelbrotSite()
    application = service.Application("Interactive Mandelbrot Set HTTP Server")
    internet.TCPServer(MY_PORT, site).setServiceParent(application)
