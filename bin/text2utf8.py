#!/usr/bin/env python3

import os
import re
import typer
import cchardet as chardet

def text2utf8(input_file: str, output_file):
    """Convert text files to utf8 with Linux file endings"""

    # TODO: Test å bruke denne heller enn/i tillegg til replace under:
    #       https://ftfy.readthedocs.io/en/latest/

    repls = (
        ('‘', 'æ'),
        ('›', 'ø'),
        ('†', 'å'),
        ('&#248;', 'ø'),
        ('&#229;', 'å'),
        ('&#230;', 'æ'),
        ('&#197;', 'Å'),
        ('&#216;', 'Ø'),
        ('&#198;', 'Æ'),
        ('=C2=A0', ' '),
        ('=C3=A6', 'æ'),
        ('=C3=B8', 'ø'),
        ('=C3=A5', 'å'),
    )

    with open(output_file, "wb") as file:
        with open(input_file, 'rb') as file_r:
            content = file_r.read()
            if content is None:
                return ""

            WINDOWS_LINE_ENDING = b'\r\n'
            UNIX_LINE_ENDING = b'\n'
            content = content.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)

            char_enc = chardet.detect(content)['encoding']

            try:
                data = content.decode(char_enc)
            except Exception:
                return ""

            for k, v in repls:
                data = re.sub(k, v, data, flags=re.MULTILINE)
        file.write(data.encode('utf8'))

        if os.path.exists(output_file):
            return output_file

    return ""

if __name__ == "__main__":
    typer.run(text2utf8)
