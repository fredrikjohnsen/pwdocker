#!/usr/bin/env python3

import os
import typer

def pdf2text(file_path):

    split_ext = os.path.splitext(file_path)
    output_file = split_ext[0] + '.txt'

    command = 'gs -sDEVICE=txtwrite -q -dNOPAUSE -dBATCH -sOutputFile="' + output_file + '" "' + file_path + '"'
    # print('command', command)
    try:
        os.system(command)
    except:
        return False

    return True

if __name__ == "__main__":
    typer.run(pdf2text)
