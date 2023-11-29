#!/usr/bin/env bash

for f in "${@:1:$#-1}"; do
    base_name=$(basename ${f})
    echo "base_name: $base_name"
    # Treat last argument as folder if more than 2 arguments
    if [ ${#} -gt 2 ]; then
        out="${@: -1}/$base_name"
    else
        out="$2"
    fi
    echo "out: $out"

    gs -q -dPDFA=2 -dBATCH -dNOPAUSE -sProcessColorModel=DeviceRGB -sDEVICE=pdfwrite -dPDFACompatibilityPolicy=1 -sOutputFile="$out" "$f"
done
