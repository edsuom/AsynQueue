#!/bin/sh

MCM_HOME=/home/mcm

rsync -auv --delete asynqueue/ ${MCM_HOME}/asynqueue/
rsync -auv --delete mcmandelbrot/ $MCM_HOME/mcmandelbrot/

