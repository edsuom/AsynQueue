# -*- mode: Python; -*-
# You can run this .tac file directly with:
#    twistd -ny mcm.tac

"""
Twisted Application Configuration file for a mcMandelbrot server.
"""

from twisted.application import service

from mcmandelbrot import wire


# tcp:port[:interface]
DESCRIPTION = b"unix:/home/mcm/socket"
#DESCRIPTION = b"tcp:port=1978:interface=127.0.0.1"


def get_mcmWireService():
    """
    Returns a Twisted C{endpoint} service for Mandelbrot Set images
    that you can use to create an application object.

    The service responds to connections as specified by the global
    I{DESCRIPTION} variable at the top of this configuration file and
    accepts 'image' commands via AMP to produce PNG images of the
    Mandelbrot set in a particular region of the complex plane.

    The default I{DESCRIPTION} is for a TCP server listening on the
    localhost interface at port C{1978}, which is the year in which
    the first computer image of the Mandelbrot set was generated. If
    you're confident in the security of your firewall and this code
    for some reason, you can just leave off everything after the port
    number.
    """
    mwu = wire.MandelbrotWorkerUniverse()
    ws = wire.WireServer(DESCRIPTION, mwu)
    return ws.service


# Create the root-level application object
application = service.Application("mcm")

# Get the mcmWireService and attach it to its parent application
mcmWireService = get_mcmWireService()
mcmWireService.setServiceParent(application)
