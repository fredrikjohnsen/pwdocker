#!/usr/bin/env python3

import os

from typing import List

from bin.pdf2pdfa import pdf2pdfa
from util import run_shell_cmd, remove_file


def is_conversion_success(status_code: int, file_path: str):
    return os.path.exists(file_path) and status_code == 0


def remove_tmp_and_exit(status_code: int, file_path: str):
    remove_file(file_path)
    if status_code != 0:
        return 1


def run_command_and_convert_to_pdfa(command: List[str], tmp_path: str, target_path: str):
    initial_convert_status_code, out, err = run_shell_cmd(command)[0]
    if not is_conversion_success(initial_convert_status_code, tmp_path):
        remove_tmp_and_exit(initial_convert_status_code, tmp_path)

    status_code = pdf2pdfa(tmp_path, target_path, '1.0')
    if not is_conversion_success(status_code, target_path):
        remove_tmp_and_exit(status_code, tmp_path)

    remove_file(tmp_path)
    return 0
