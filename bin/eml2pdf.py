#!/usr/bin/env python3

import os
import sys

import typer

from bin import pdf2pdfa
from bin.utils import remove_file
from convert import run_shell_command


def eml2pdf(src_file_path: str, target_file_path: str):
    """
    Convert email content to pdf/a

    Args:
        src_file_path: path for the file to be converted
        target_file_path: path for the converted file

    Returns:
        Exit code 0 if successful, otherwise 1.
    """
    tmp_file = f"{os.path.dirname(os.path.realpath(src_file_path))}/tmp.pdf"
    command = ['eml_to_pdf', src_file_path, tmp_file]

    initial_convert_status_code = run_shell_command(command)[0]
    if not is_conversion_success(initial_convert_status_code, tmp_file):
        remove_tmp_and_exit(initial_convert_status_code, tmp_file)

    status_code = pdf2pdfa(tmp_file, target_file_path)
    if not is_conversion_success(status_code, target_file_path):
        remove_tmp_and_exit(status_code, tmp_file)

    remove_file(tmp_file)
    return sys.exit(0)


def is_conversion_success(status_code: int, file_path: str):
    return os.path.exists(file_path) and status_code == 0


def remove_tmp_and_exit(status_code: int, file_path: str):
    remove_file(file_path)
    if status_code != 0:
        sys.exit(1)


if __name__ == "__main__":
    typer.run(eml2pdf)
