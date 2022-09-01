#!/usr/bin/env python3
import os
import sys
import psutil
import subprocess
import time
from pathlib import Path

import typer
from unoserver import converter

# from pdf2pdfa import pdf2pdfa


def start_uno_server():
    if uno_server_running():
        return

    subprocess.Popen(
        Path(Path.home(), ".local/bin/unoserver"),
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    time.sleep(3)  # Wait for server to start up


def uno_server_running():
    for process in psutil.process_iter():
        if process.name() == "unoserver":
            return True

    return False


def unoconv2x(source_path: str, target_path: str):
    """
    Convert spreadsheet, MS Word or rtf files to pdf or html.
    Spreadsheet files can be converted to html or pdf specified with the extension of target_path.
    Word and rtf files can only be converted to pdf.

    Example: python3 -m bin.unoconv2x ./example/testfile.xls ./result pdf
    application/vnd.openxmlformats-officedocument.spreadsheetml.sheet

    Supported mime types:
        - application/vnd.ms-excel
        - application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
        - application/msword
        - application/rtf

    Args:
        source_path: path for the file to be converted
        target_path: path for the converted file
    Returns:
        Nothing if successful otherwise exits with exit code 1
    """

    start_uno_server()
    target_ext = Path(target_path).suffix
    _converter = converter.UnoConverter()
    result = _converter.convert(source_path, None, target_path, target_ext)

    if not Path(target_path).is_file() or result is not None:
        sys.exit(1)


if __name__ == "__main__":
    typer.run(unoconv2x)
