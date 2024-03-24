#!/usr/bin/env python3

import os
from pathlib import Path
import sys
import typer
import uuid

from bin.common import run_command_and_convert_to_pdfa
from util import run_shell_cmd


def email2pdf(src_file_path: str, target_file_path: str):
    """
    Convert email content to pdf/a

    Args:
        src_file_path: path for the image to be converted
        target_file_path: path for the converted file

    Returns:
        Exit code 0 if successful, otherwise 1.
    """
    rel_path = os.path.relpath(src_file_path)
    tmp_file = f"{os.path.dirname(rel_path)}/{uuid.uuid4()}.pdf"
    r_command = ['eml_to_pdf', src_file_path, tmp_file]
    run_shell_cmd(r_command)
    # TODO: Den over for selve mailen og den under for å håndtere vedlegg
    # -> må zippe til slutt da
    # -> sjekk først om finnes vedlegg da?

    # TODO: Smarteste måten disse kan kombineres?

    tmp_file = f"{os.path.dirname(rel_path)}/{uuid.uuid4()}.pdf"
    jar_file = f"{Path.home()}/bin/emailconvert/emailconverter.jar"
    j_command = ['java', '-jar', jar_file, src_file_path, "-o", tmp_file]
    return sys.exit(run_command_and_convert_to_pdfa(j_command, tmp_file,
                                                    target_file_path))


if __name__ == "__main__":
    typer.run(email2pdf)
