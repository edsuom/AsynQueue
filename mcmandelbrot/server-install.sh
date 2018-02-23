#!/bin/sh

# Assumes existence of a non-privileged user 'mcm' with group 'mcm'
# and home directory /home/mcm/
cp mcmandelbrot/mcm.tac /home/mcm/
cp mcmandelbrot/mcm.service /etc/systemd/system/

systemctl daemon-reload
systemctl enable mcm.service
