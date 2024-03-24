#!/usr/bin/env python3

import sys
from pathlib import Path

import typer

from util import run_shell_cmd


def office2pdf(source_file: str, target_file: str):
    """
    Convert office files to pdf

    Args:
        source_file: path for the file to be converted
        target_file: path for the converted file

    Returns:
        Nothing
    """

    docbuilder_file = Path('/tmp', 'x2x.docbuilder')

    docbuilder = [
        f'builder.OpenFile("{source_file}", "")',
        f'builder.SaveFile("pdf", "{target_file}")',
        'builder.CloseFile();',
    ]

    with open(docbuilder_file, 'w+') as file:
        file.write('\n'.join(docbuilder))

    command = ['documentbuilder', docbuilder_file]
    result, out, err = run_shell_cmd(command)

    if err:
        print('err', err)
        sys.exit(1)


if __name__ == '__main__':
    typer.run(office2pdf)
