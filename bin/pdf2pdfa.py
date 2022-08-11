#!/usr/bin/env python3

import sys

import ocrmypdf
from ocrmypdf import Verbosity, ExitCodeException
import typer


def pdf2pdfa(input_file: str, output_file: str, timeout: int = 180):
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
