#!/bin/bash

# Prepare libraries for ParaMark usage
# Debian/Ubuntu

PKGS="python python-scipy python-numpy python-gnuplot python-htmlgen
gnuplot"

sudo apt-get install $PKGS
