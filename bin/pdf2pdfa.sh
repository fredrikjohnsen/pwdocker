#!/usr/bin/env bash

gs -q -dPDFA=2 -dBATCH -dNOPAUSE -sProcessColorModel=DeviceRGB -sDEVICE=pdfwrite -dPDFACompatibilityPolicy=1 -sOutputFile=$2 $1
