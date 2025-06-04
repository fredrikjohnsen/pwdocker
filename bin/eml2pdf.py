#!/usr/bin/env python3

import os
import sys
import typer
import uuid

from bin.common import run_command_and_convert_to_pdfa


def eml2pdf(src_file_path: str, target_file_path: str):
    """
    Convert email content to pdf/a

    Args:
        src_file_path: path for the image to be converted
        target_file_path: path for the converted file

    Returns:
        Exit code 0 if successful, otherwise 1.
    """
    tmp_file = f"{os.path.dirname(os.path.realpath(src_file_path))}/{uuid.uuid4()}.pdf"
    command = ['eml_to_pdf', src_file_path, tmp_file]
    return sys.exit(run_command_and_convert_to_pdfa(command, tmp_file, target_file_path))


if __name__ == "__main__":
    typer.run(eml2pdf)
