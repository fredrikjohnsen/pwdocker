#!/usr/bin/env python3

import sys
import typer
from pdfy import Pdfy


# TODO: Denne fungerer ikke.
def html2pdf(src_file_path: str, target_file_path: str):
    """Convert html to pdf"""
    norm_file_path = ""
    try:
        p = Pdfy()
        p.html_to_pdf(src_file.path, tmp_file.path)
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == '__main__':
    typer.run(html2pdf)
