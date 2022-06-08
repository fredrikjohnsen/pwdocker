#!/usr/bin/env python3

import os
import typer

def office2pdf(source_file: str, target_file: str):
    """Convert office files to pdf"""
    target_dir = os.path.dirname(target_file)
    docbuilder_file = os.path.join(target_dir, 'x2x.docbuilder')

    docbuilder = [
        'builder.OpenFile("' + source_file + '", "")',
        'builder.SaveFile("pdf", "' + target_file + '")',
        'builder.CloseFile();',
    ]

    with open(docbuilder_file, "w+") as file:
        file.write("\n".join(docbuilder))

    command = ['documentbuilder', docbuilder_file]
    run_shell_command(command)

    if os.path.isfile(docbuilder_file):
        os.remove(docbuilder_file)


if __name__ == "__main__":
    typer.run(office2pdf)
