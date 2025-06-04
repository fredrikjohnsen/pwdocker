#!/usr/bin/env python3

import typer
import pdfkit


def html2pdf(src_file_path: str, target_file_path: str):
    """
    Convert html content to pdf

    Args:
        src_file_path: path for the file to be converted
        target_file_path: path for the converted file

    Returns:
        True on success
    """
    try:
        result = pdfkit.from_file(input=src_file_path, output_path=target_file_path, verbose=True)
        return True if result is True else False
    except IOError as e:
        print(e)
        return False


if __name__ == '__main__':
    typer.run(html2pdf)
