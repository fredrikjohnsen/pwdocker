#!/usr/bin/env python3

import os
import shutil
import sys

import ocrmypdf
from ocrmypdf import Verbosity, ExitCodeException
import typer


def pdf2pdfa(input_file: str, output_file: str, version: str = None, timeout: int = 0):
    """
    Convert pdf to pdf/a

    By default, does OCR, this can be disabled by setting timeout to 0.

    Args:
        input_file: path for the file to be converted
        output_file: path for the converted file
        timeout: Set to 0 to only do pdf/a-conversion and not ocr

    Returns:
        exit code
    """

    if version in ('1a', '1b', '2a', '2b'):
        shutil.copy(input_file, output_file)
        if os.path.exists(output_file):
            return 0

    ocrmypdf.configure_logging(Verbosity.quiet)
    try:
        exit_code = ocrmypdf.ocr(input_file, output_file, tesseract_timeout=timeout, progress_bar=False, skip_text=True)
    except ExitCodeException as e:
        print(e)
        sys.exit(1)

    # value of IntEnum: ocrmypdf.ExitCode
    return exit_code.value


if __name__ == '__main__':
    typer.run(pdf2pdfa)
