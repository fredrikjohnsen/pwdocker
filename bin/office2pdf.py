#!/usr/bin/env python3

import os
from pathlib import Path

import typer

from util import run_shell_command, remove_file


def office2pdf(source_file: str, target_file: str):
    """
    Convert office files to pdf

    Args:
        source_file: path for the file to be converted
        target_file: path for the converted file

    Returns:
        Nothing
    """
    target_dir = os.path.dirname(target_file)
    docbuilder_file = Path('/tmp', 'x2x.docbuilder')

    docbuilder = [
        f'builder.OpenFile("{source_file}", "")',
        f'builder.SaveFile("pdf", "{target_file}")',
        'builder.CloseFile();',
    ]

    with open(docbuilder_file, 'w+') as file:
        file.write('\n'.join(docbuilder))

    command = ['documentbuilder', docbuilder_file]
    result, out, err = run_shell_command(command)

    if result:
        raise Exception("Conversion failed")

if __name__ == '__main__':
    typer.run(office2pdf)
