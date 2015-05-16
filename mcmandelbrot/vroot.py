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
The L{HTML_VRoot} combines with the L{HTML_Baton} to provide a
powerful way to generate an HTML page. Adapted from another project
for use in the C{mcmandelbrot} demo site.
"""

import os.path, sys, re, traceback, codecs
from pkg_resources import resource_stream

from xml.dom import minidom
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import SubElement


def newElement(tag, parent=None):
    if parent is None:
        return ET.Element(tag)
    return SubElement(parent, tag)

def dedent(lines, padding):
    result = []
    kChar = min([len(x)-len(x.lstrip()) for x in lines])
    for line in lines:
        result.append(u" "*padding + line[kChar:])
    return result

def dedentString(text, padding):
    lines = text.split('\n')
    return '\n'.join(dedent(lines, padding))

def openPackageFile(fileName):
    filePath = os.path.join(
        os.path.dirname(__file__), fileName)
    if os.path.exists(filePath):
        return open(filePath)
    return resource_stream(__name__, fileName)


class Baton(object):
    """
    The VRoot gives an instance of me a reference e to a virtual root
    element and then passes my instance to a context caller. The
    caller can use my convenience methods to generate XML subelements
    of the virtual root element, with placeholders for raw
    xml/html. Then, when the caller is done, I convert the tree from
    the virtual root element into xml/html.
    """
    reInvalidXML = re.compile(r'[\<\>\&]')
    reContentPara = re.compile(r'<p>(.+)</p>\s*$')
    reLeadingSpace = re.compile(r'\n*</[^>]+>\n*(\s*?)<')

    def __init__(self, indent):
        self.indent = indent
        self.elements = []
        self.eChild = None
        self.lastParent = None
        self.e = self.seParent = newElement('vroot')
        self.uniquePrefix = "!UPH-{:d}".format(id(self))
        self.rePlaceholder = re.compile(
            r'{}-([0-9]+)!'.format(self.uniquePrefix))

    def info(self, e=None):
        info = ""
        if e is None:
            e = self.eChild
        if e is None:
            return "<No child yet>"
        klasses = e.get('class', "")
        if klasses:
            info += ' class="{}"'.format(klasses)
        text = e.text if e.text else ""
        result = "<{}{}>{}</{}>".format(
            e.tag, info, text, e.tag)
        children = list(e)
        if children:
            result += ": [{}]".format(", ".join(
                    [self.info(x) for x in children]))
        return result

    def __repr__(self):
        return "Last child: {}".format(self.info())

    def parent(self, e):
        """
        Returns the parent of the supplied element.
        """
        xpath = ".//{}/..".format(e.tag)
        for possibleParent in self.e.findall(xpath):
            if e in list(possibleParent):
                return possibleParent

    def parentAndClassFromArgs(self, args):
        klass = None
        parent = self.e if self.eChild is None else self.eChild
        for arg in args:
            if ET.iselement(arg):
                # A parent was specified, don't use last child's parent
                parent = arg
            elif isinstance(arg, (str, unicode)):
                # A class was specified
                klass = arg
        return parent, klass

    def se(self, tag, klass=None):
        """
        Convenience method to return a subelement of the last child I
        generated (from a subclass entryMethod) before being offered
        to the context caller. You can use this to generate top-level
        items within the context of your call to a VROOT.

        You can specify a CSS class for the new subelement.

        DO NOT USE this method in a VROOT's entryMethod! Doing so will
        screw up the lastParent. It is for context callers.
        """
        self.lastParent = self.seParent
        e = self.eChild = newElement(tag, self.seParent)
        if klass:
            e.set('class', klass)
        return e

    def nc(self, tag, *args):
        """
        Generates a new child of the last-generated child, another
        parent specified as an argument, or my root if this is the
        first child I've generated.

        Repeated calls to this method without parents specified will
        result in a nested hierarchy.

        If a class name is specified via a string argument, the new
        child gets that set. The order of the arguments doesn't
        matter, and neither or both can be specified.

        Returns a reference to the new element and saves it as the
        last-generated child.
        """
        parent, klass = self.parentAndClassFromArgs(args)
        self.lastParent = parent
        e = self.eChild = newElement(tag, parent)
        if klass:
            e.set('class', klass)
        return e

    def nci(self, iterator, tag, *args):
        """
        Iterates over the supplied iterator to generate children of the
        last-generated child, another parent specified as an argument,
        or my root if this is the first child I've generated. 

        Each child generated will be considered the last-generated one
        within its iteration, and only there. Children iterated after
        the first one will be siblings to the first.

        If a class name is specified via a string argument, each new
        child gets that set. The order of the arguments doesn't
        matter, and neither or both can be specified.

        For each iteration, generates a new child of the parent and
        yields the value of the original iteration. The new child will
        be treated the last-generated child for that iteration, and
        only that iteration.

        It's expected that you will use stuff like nc to make further
        children (i.e., grandchilden of the last-generated child)
        within each iteration, but after the iterations are done, the
        last-generated child will be restored to where it was before
        the method call.
        """
        def eNew():
            self.eChild = newElement(tag, parent)
            if klass:
                self.eChild.set('class', klass)

        parent, klass = self.parentAndClassFromArgs(args)
        self.lastParent = parent
        self._nciChild = self.eChild
        if callable(iterator):
            for x in iterator():
                eNew()
                yield x
        else:
            for x in iterator:
                eNew()
                yield x
        self.eChild = self._nciChild
    
    def ns(self, tag, *args):
        """
        Generates a new child of THE PARENT to the last-generated
        child or another sibling element specified. The new child will
        then be considered the last-generated one.

        Repeated calls to this method (with or without a sibling
        element specified) will result in siblings rather than a nested
        hierarchy, because the parent doesn't change even if the child
        does.

        If a class name is specified, the new child gets that set.

        Returns a reference to the new element and saves it as the
        last-generated child.
        """
        sibling, klass = self.parentAndClassFromArgs(args)
        if sibling not in (self.e, self.eChild):
            # A sibling was supplied in the args, use its parent
            self.lastParent = self.parent(sibling)
        parent = self.lastParent
        e = self.eChild = newElement(tag, parent)
        if klass:
            e.set('class', klass)
        # Note that self.lastParent does not change, unless a sibling
        # was specified. Then its parent will be the new lastParent,
        # for further calls to this method without needing to supply
        # the sibling again.
        return e

    def nu(self, tag, klass=None):
        """
        Generates a new uncle of the last-generated child. This will
        then be considered the last-generated "child." If a class name
        is specified, the new element gets that set.

        Useful for making one or more children of an element, and then
        going on to make a sibling of that element.
        """
        return self.ns(tag, self.parent(self.eChild), klass)

    def p(self, content, *args):
        """
        Generates a paragraph element as a new child, with either the
        supplied content as its text, or, if the content is wrapped in
        paragraph tags, a placeholder for substituting (without the
        <p> tags) after serialization.

        The new element is a child of the last-generated child, unless
        the last child was also a paragraph. Then the new element is a
        sibling of that paragraph. You can also specify another parent
        as an argument. The root is used if this is the first child
        I've generated. Repeated calls to this method will produce
        sibling paragraphs, not a nested hierarchy.

        If a class name is specified via a string argument, the
        paragraph element gets that set. The order of any parent/klass
        arguments doesn't matter, and neither or both can be
        specified.
        """
        parent, klass = self.parentAndClassFromArgs(args)
        if getattr(self.eChild, 'tag', None) == 'p':
            parent = self.lastParent
        else:
            self.lastParent = parent
        e = self.eChild = newElement('p', parent)
        if klass:
            e.set('class', klass)
        content = re.sub(r'\s*\n+\s*', ' ', content)
        match = self.reContentPara.match(content)
        if match:
            content = self.np(match.group(1))
        e.text = content
        return e

    def ngc(self, tag, *args):
        """
        Generates a new subelement (somebody's grandchild) of the child
        last generated with the se, nc, ns, or p method without
        changing its status as the last child generated. You can
        specify a parent and class, as usual, in either order.

        Repeated calls to this method will result in a succesion of
        grandchildren with the same ancestors, not a nested hierarchy.

        If a class name is specified, the new element gets that
        set.

        Returns a reference to the new element without saving it
        anywhere.
        """
        parent, klass = self.parentAndClassFromArgs(args)
        eGrandChild = newElement(tag, parent)
        if klass:
            eGrandChild.set('class', klass)
        return eGrandChild

    def rp(self, parent=None):
        """
        Reset parent to use for the next child I generate, to the one
        specified or my seParent. Returns a reference to me (not the
        parent) for convenience of methods called by my context caller
        and using me to build stuff.
        """
        if parent is None:
            parent = self.seParent
        self.eChild = self.lastParent = parent
        return self

    def np(self, content):
        """
        Assigns a unique placeholder for the supplied content string
        and stores it to be replaced back during
        serialization. Returns the placeholder.
        """
        ph = "{}-{:d}!".format(
            self.uniquePrefix, len(self.elements))
        self.elements.append(content)
        return ph

    def possiblyPlacehold(self, e, content, attrName='text'):
        """
        If the supplied xml/html content string contains a character
        that must be escaped in XML (e.g., "<"), assigns a unique
        placeholder for the entire string and stores the content to be
        replaced back during serialization.
        """
        if self.reInvalidXML.search(content):
            content = self.np(content)
        setattr(e, attrName, content)

    def text(self, content, e=None):
        """
        Sets the text value of the last-generated child (or an element
        specified) to the supplied content, with XML escaping if
        needed.
        """
        if e is None:
            e = self.eChild
        e.text = content

    def textX(self, content, e=None):
        """
        Sets the text value of the last-generated child (or an element
        specified) to the supplied content, with a placeholder if
        needed to preserve raw XML.
        """
        if e is None:
            e = self.eChild
        if e.tag in ('div'):
            content = content.strip()
            content = "\n" + content + "\n"
        self.possiblyPlacehold(e, content)

    def tail(self, content, e=None):
        """
        Sets the tail value of the last-generated child (or an element
        specified) to the supplied content, with XML escaping if
        needed.
        """
        if e is None:
            e = self.eChild
        e.tail = content

    def tailX(self, content, e=None):
        """
        Sets the tail value of the last-generated child (or an element
        specified) to the supplied content, with a placeholder if
        needed to preserve raw XML.
        """
        if e is None:
            e = self.eChild
        self.possiblyPlacehold(e, content, 'tail')

    def set(self, name, value):
        """
        Sets an attribute of my last-generated child element.
        """
        self.eChild.set(name, value)

    def meta(self, *args, **kw):
        """
        Adds a meta tag as a new child, to the last child or a parent
        specified as a single argument, using the keywords supplied.

        The meta tag becomes the new last child.
        """
        parent = args[0] if args else None
        self.nc('meta', parent)
        for name, value in kw.iteritems():
            name = name.replace('_', '-')
            self.set(name, value)
        
    def margin(self, side, em):
        """
        Convenience method to set a margin of my last-generated child
        element. Specify in em with a float.
        """
        style = [self.eChild.get('style', "").strip()]
        style.append("margin-{}: {:1.1f}em;".format(side, em))
        self.set('style', " ".join(style).strip())

    def setxml(self, xml):
        """
        Sets my xml attribute with the supplied xml, after
        substituting out any placeholders.
        """
        while True:
            match = self.rePlaceholder.search(xml)
            if not match:
                break
            k = int(match.group(1))
            before = xml[:match.start(0)]
            after = xml[match.end(0):]
            replacement = self.elements[k]
            xml = before + replacement + after
        self.xml = xml

    def get(self):
        """
        Returns my eTree.
        """
        return self.e
        

class VRoot(object):
    """
    I am a context manager for passing you a baton with a virtual root
    element (my "e" attribute) to which you can add elements, e.g., an
    HTML document. When you get done, I'll put an XML string in the
    baton. You can strip out the XML tag. You can also specify a tag
    for wrapping the stripped content (e.g., "<html>...</html>").

    The baton has tons of convenience methods for generating tags.

    with (me) as vroot:
        ncx = vroot.se('ncx')
        ...
    xml = vroot.xml [ or = vroot() ]
    ...

    Call my instance to get the XML or HTML as a string.

    """
    # Number of spaces to indent each XML level
    indent = 2  

    replaceXML = None
    versionXML = 1.0

    reEmptyItem = re.compile(
        r'<([a-zA-Z\:]+)\s+[^>]*[^/>](>)</([a-zA-Z\:]+)>')

    def fixCloseTags(self, text):
        while True:
            match = self.reEmptyItem.search(text)
            if not match:
                break
            if match.group(1) != match.group(3):
                raise ValueError(
                    "Malformed XML in substring '{}'".format(match.group(0)))
            text = text[:match.start(2)] + " />"
        return text

    def xmlToUnicode(self):
        """
        Return a pretty-printed XML string for my vroot element. From
        http://pymotw.com/2/xml/etree/ElementTree/create.html
        """
        kw = {}
        if getattr(self, 'encoding', None):
            kw['encoding'] = self.encoding
        rough_string = ET.tostring(self.b.get(), **kw)
        reparsed = minidom.parseString(rough_string)
        kw['indent'] = " " * self.indent
        xml = reparsed.toprettyxml(**kw)
        if isinstance(xml, str):
            # A string with ascii-encoded unicode was returned from
            # Et.tostring, so decode it into a unicode object for
            # consistency
            xml = codecs.decode(xml, 'utf-8')
        return self.fixCloseTags(xml)

    def __call__(self):
        xml = getattr(getattr(self, 'b', None), 'xml', None)
        return bytes(xml)

    def stripVroot(self, xml):
        return re.sub(r"</?vroot>", r"", xml)

    def __enter__(self):
        self.b = Baton(self.indent)
        for name in ('version', 'xmlns'):
            value = getattr(self, name, None)
            if value:
                setattr(self.b, name, value)
        # Possible custom entry method
        self.entryMethod(self.b)
        if getattr(self.b, 'eChild', None) is not None:
            self.b.seParent = self.b.lastParent = self.b.eChild
        return self.b

    def __exit__(self, etype, value, trace):
        if etype is not None:
            try:
                xml = self.xmlToUnicode(self)
            except:
                xml = u"<Invalid>"
            print u"\nIncomplete Vroot Element: {}\n{}\n".format(
                xml, '-'*79)
            traceback.print_exception(etype, value, trace)
            sys.exit(1)
        # Pretty-print version of my element's XML
        xml = ET.tostring(self.b.get())
        # Strip vroot tags
        html = "<html>\n{}\n</html>".format(self.stripVroot(xml))
        # Put html in baton
        self.b.setxml(html)


class HV_Meta(type):
    """
    Load class-wide lists of lines for CSS and JS
    """
    def __new__(cls, name, parents, dct):
        dct['headLines'] = {}
        for fileType, fileName in dct['headFiles'].iteritems():
            lines = []
            fh = openPackageFile(fileName)
            for line in fh:
                if line.strip():
                    lines.append(line.rstrip())
            fh.close()
            dct['headLines'][fileType] = lines
        return super(HV_Meta, cls).__new__(cls, name, parents, dct)
            
            
class HTML_VRoot(VRoot):
    """
    L{VRoot} for HTML.
    """
    __metaclass__ = HV_Meta
    
    charset = "utf-8"
    replaceXML = "html"

    headFiles = {'css': "mcm.css", 'js': "mcm.js"}

    def __init__(self):
        self.metaTags = [
            {'name':"viewport",
             'content':"width=device-width, initial-scale=1"}]

    def insert(self, v, tag, name, typ=None):
        """
        Inserts the text read from the named entry in I{headLines}.
        """
        v.ns(tag)
        v.textX(u"\n"+u"\n".join(self.headLines[name]))
        if typ:
            v.set('type', typ)

    def head(self, v):
        h = v.nc('head')
        v.nc('title')
        # There might be HTML formatting chars in the title string
        v.textX(self.title)
        # Meta
        v.meta(
            h, charset=self.charset,
            content="application/xhtml+xml",
            http_equiv="Content-Type")
        for kw in self.metaTags:
            v.meta(h, **kw)
        self.insert(v, 'style', 'css')
        self.insert(v, 'script', 'js', "text/javascript")
    
    def entryMethod(self, v):
        self.head(v)
        # BODY/DIV
        v.rp()
        v.nc('body')
        v.set('onload', "updateImage()")
        v.nc('div', 'container')
        # The rest is up to the context caller
