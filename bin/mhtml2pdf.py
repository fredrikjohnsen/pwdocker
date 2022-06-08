#!/usr/bin/env python3

import os
import typer

def mhtml2pdf(source_file: str, target_file: str):
    """Convert archived web content to pdf"""
    java_path = os.environ['pwcode_java_path']  # Get Java home path
    converter_jar = os.path.expanduser("~") + '/bin/emailconverter/emailconverter.jar'

    command = [java_path, '-jar', converter_jar, '-e', source_file, '-o', target_file]
    result = run_shell_command(command)
    # print(result)


if __name__ == "__main__":
    typer.run(mhtml2pdf)
