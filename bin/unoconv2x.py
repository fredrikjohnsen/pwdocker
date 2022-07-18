#!/usr/bin/env python3
import os
import sys
import typer

from convert import run_shell_command


def unoconv2x(source_path: str, target_path: str, target_ext: str, mime_type: str):
    """
    Convert spreadsheet, MS Word or rtf files to pdf or html.
    Spreadsheet files can be converted to html or pdf specified with the @param target_ext
    Word and rtf files can only be converted to pdf.

    Example: python3 -m bin.unoconv2x ./example/testfile.xls ./result pdf
    application/vnd.openxmlformats-officedocument.spreadsheetml.sheet

    Supported mime types:
        - application/vnd.ms-excel
        - application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
        - application/msword
        - application/rtf

    Args:
        source_path: path for the file to be converted
        target_path: path for the converted file
        target_ext: extension for the file-format to convert to
        mime_type: the mime-type of the source file
    Returns:
        Nothing if successful otherwise exits with exit code 1
    """
    command = ['unoconv', '-f', target_ext]

    if mime_type in (
            'application/vnd.ms-excel',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ):
        if target_ext == 'pdf':
            command.extend([
                '-d', 'spreadsheet', '-e PaperOrientation=landscape', '-e SelectPdfVersion=1'
            ])
        elif target_ext == 'html':
            command.extend(
                ['-d', 'spreadsheet', '-e PaperOrientation=landscape'])
    elif mime_type in ('application/msword', 'application/rtf'):
        command.extend(['-d', 'document', '-e SelectPdfVersion=1'])

    command.extend(['-o', target_path, source_path])
    result = run_shell_command(command)
    status_code = result[0]

    if not os.path.exists(target_path) or status_code != 0:
        sys.exit(1)


if __name__ == "__main__":
    typer.run(unoconv2x)
