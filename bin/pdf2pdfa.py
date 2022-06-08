#!/usr/bin/env python3

import sys
import typer
import ocrmypdf

def pdf2pdfa(input_file: str, output_file: str, timeout: int = 180):
    """
    Convert pdf to pdf/a

    Use --timeout 0  to only do pdf/a-conversion, and not ocr
    """

    ocrmypdf.configure_logging(-1)
    try:
        result = ocrmypdf.ocr(input_file, output_file,
                              tesseract_timeout=timeout, progress_bar=False, skip_text=True)
    except Exception as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    typer.run(pdf2pdfa)
