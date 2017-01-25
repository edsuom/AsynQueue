#!/usr/bin/python
# -*- coding: utf-8 -*-
# UTF-8. That’s cool!

import re

import xml.etree.ElementTree as ET
from xml.etree.ElementTree import SubElement as se

from testbase import TestCase

# Module under test
from mcmandelbrot import vroot


VERBOSE = True


# Illegal, wrapped in <p>, text
CONTENT = (
    (False, False,
     "Foo! This paragraph actually has no "+\
         "illegal characters? Truly: It is so.",),
    (True, False,
     "foo < bar. A shame? This one does have something illegal"),
    (True, True,
     "<p>Wrapped in paragraph tags but otherwise nothing illegal.</p>"))


class TestBaton(TestCase):
    def setUp(self):
        self.v = vroot.Baton(2)

    def test_repr(self):
        vrep = str(self.v)
        self.assertIn("No child", vrep)
        self.v.nc('div', 'myclass')
        self.v.text("foo")
        vrep = str(self.v)
        self.assertPattern(r'<div\s+class="myclass"\s*>foo</div>', vrep)

    def test_parentAndClassFromArgs(self):
        # Parent is root because no children have been generated yet
        p, k = self.v.parentAndClassFromArgs([])
        self.assertEqual(p, self.v.e)
        # Parent is the last child generated
        e1 = self.v.se('1')
        p, k = self.v.parentAndClassFromArgs([])
        self.assertEqual(p, e1)
        # If a parent is specified as an arg, that will be it
        e2 = self.v.nc('2a')
        p, k = self.v.parentAndClassFromArgs(['foo', e1])
        self.assertEqual(p, e1)

    def test_nc(self):
        child = self.v.nc('1')
        grandchild_1 = self.v.nc('2a')
        grandchild_2 = self.v.nc('2b', child)
        self.assertEqual(list(self.v.e), [child])
        self.assertEqual(list(child), [grandchild_1, grandchild_2])

    def test_nci(self):
        kLast = -1
        eLast = None
        siblings = []
        grandchildren = []
        eChildBeforeIteration = self.v.eChild
        for k in self.v.nci(xrange(5), '1'):
            self.assertEqual(k, kLast+1)
            kLast = k
            thisChild = self.v.eChild
            self.assertNotEqual(thisChild, eLast)
            eLast = thisChild
            siblings.append(thisChild)
            grandchildren.append(self.v.nc('2'))
        self.assertEqual(self.v.eChild, eChildBeforeIteration)
        self.assertEqual(list(self.v.e), siblings)
        for k, child in enumerate(siblings):
            self.assertEqual(child[0], grandchildren[k])
        
    def test_ns(self):
        parent = self.v.se('1')
        child_1 = self.v.nc('2a')
        child_2 = self.v.ns('2b')
        self.assertEqual(list(parent), [child_1, child_2])
        grandchild_1 = self.v.nc('3a')
        grandchild_2 = self.v.ns('3b')
        self.assertEqual(list(child_1), [])
        self.assertEqual(list(child_2), [grandchild_1, grandchild_2])
        # Make a sibling of the parent
        uncle = self.v.ns('1b', parent)
        self.assertEqual(list(self.v.e), [parent, uncle])
        
    def test_ngc(self):
        parent = self.v.se('1')
        child_1 = self.v.nc('2a')
        grandchild_1 = self.v.ngc('3a')
        child_2 = self.v.ns('2b')
        grandchild_2 = self.v.ngc('3b')
        grandchild_3 = self.v.ngc('3c')
        self.assertEqual(list(child_1), [grandchild_1])
        self.assertEqual(list(child_2), [grandchild_2, grandchild_3])

    def test_nu(self):
        parent = self.v.se('1')
        child_1 = self.v.nc('2a')
        grandchild_1 = self.v.nc('3a')
        child_2 = self.v.nu('2b')
        self.assertEqual(list(parent), [child_1, child_2])

    def test_parent(self):
        # Build tree with v methods, as I would in real life
        parent = self.v.se('1')
        child_1 = self.v.nc('2a')
        grandchild_1 = self.v.ngc('3a')
        child_2 = self.v.ns('2b')
        grandchild_2 = self.v.ngc('3b')
        grandchild_3 = self.v.ngc('3c')
        # Test parent finding
        for child in (child_1, child_2):
            self.assertEqual(self.v.parent(child), parent)
        self.assertEqual(self.v.parent(grandchild_1), child_1)
        for grandchild in (grandchild_2, grandchild_3):
            self.assertEqual(self.v.parent(grandchild), child_2)

    def test_newPlaceholder(self):
        placeholders = []
        for k, stuff in enumerate(CONTENT):
            text = stuff[2]
            placeholders.append(self.v.np(text))
        for ph in placeholders:
            match = re.match(self.v.rePlaceholder, ph)
            self.assertTrue(match is not None)
            k = int(match.group(1))
            self.assertEqual(self.v.elements[k], CONTENT[k][2])

    def test_possiblyPlacehold(self):
        for k, stuff in enumerate(CONTENT):
            shouldPH = stuff[0]
            text = stuff[2]
            self.v.possiblyPlacehold(self.v.e, text)
            if shouldPH:
                self.assertIn('!UPH-', self.v.e.text)
                self.assertIn(text, self.v.elements)
            else:
                self.assertEqual(self.v.e.text, text)
                self.assertNotIn(text, self.v.elements)


class TestableVRoot(vroot.VRoot):
    def entryMethod(self, v):
        self._seParentAtEntryStart = v.seParent
        self._eTest = v.se('p')


class TestVRoot(TestCase):
    def setUp(self):
        self.v = TestableVRoot("Testing")
        
    def test_basic(self):
        b = self.v.__enter__()
        self.assertIsInstance(b, vroot.Baton)
        tagText = (('foo', "First"), ('bar', "Second"))
        with self.v as v:
            for tag, text in tagText:
                sub = v.se(tag)
                sub.set('x', "y")
                sub.text = text
            sub = v.se('p', 'classy')
            sub.text = "Classy Text"
        xml = self.v()
        if VERBOSE:
            print "\n\n", xml
        self.checkOccurrences("UTF-8", xml, 0)
        self.checkOccurrences(r'x="y"', xml, 2)
        for tag, text in tagText:
            self.checkOccurrences(
                r'<{}[^>]+>{}</{}>'.format(tag, text, tag), xml, 1)
        self.assertPattern(r'<p\s+class="classy">Classy Text</p>', xml)

    def test_p(self):
        with self.v as v:
            div = v.se('div')
            p1 = v.p("Plain text.")
            html = "<p>An HTML &ldquo;paragraph&rdquo; "+\
                "with <em>emphasis</em>!</p>"
            p2 = v.p(html)
        self.assertEqual(list(div), [p1, p2])
        xml = self.v()
        if VERBOSE:
            print "\n\n", xml
        self.assertIn(
            '<p>Plain text.</p>', xml)
        self.assertIn(
            '<p>An HTML &ldquo;paragraph', xml)
        self.checkOccurrences(r'</?em>', xml, 2)


class MockBaton(object):
    def __init__(self, indent=None):
        self.indent = indent
        self.calls = []

    def ns(self, *args, **kw):
        self.calls.append(['ns', args, kw])

    def set(self, *args, **kw):
        self.calls.append(['set', args, kw])

    def textX(self, *args, **kw):
        self.calls.append(['textX', args, kw])

    def link(self, *args):
        self.calls.append(['link', args])
            

class TestHTML_VRoot(TestCase):
    def setUp(self):
        self.vr = vroot.VRoot("Random Title")

    def test_head(self):
        self.vr.favicon = 'favicon.ico'
        with self.vr as v:
            self.vr.head(v)
        html = self.vr().lower()
        self.assertPattern(r'<style>', html)
        
    def checkHTML(self, html):
        if VERBOSE:
            print "\n\n", html
        self.checkBegins("<html>", html)
        self.checkOccurrences(r'</?html[^>]*>', html, 2)
        self.checkOccurrences(r'</?head[^>]*>', html, 2)
        self.checkOccurrences(r'</?body[^>]*>', html, 2)
        self.assertPattern(
            r'<body>[\s\n]*<p>just this one', html)

    def test_basic(self):
        with self.vr as v:
            v.nc('body')
            v.nc('p')
            v.text("Just this one paragraph.")
        html = self.vr().lower()
        self.checkHTML(html)


