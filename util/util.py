from __future__ import annotations
import csv
import re
import shutil
import signal
import subprocess
from subprocess import TimeoutExpired
import os
import zipfile
from pathlib import Path


def run_shell_command(command, cwd=None, timeout=60, shell=False) -> int:
    """
    Run the given command as a subprocess

    Args:
        command: The child process that should be executed
        cwd: Sets the current directory before the child is executed
        timeout: The number of seconds to wait before timing out the subprocess
        shell: If true, the command will be executed through the shell.
    Returns:
        exit code
    """
    os.environ["PYTHONUNBUFFERED"] = "1"

    proc = subprocess.Popen(
        command,
        cwd=cwd,
        shell=shell,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=os.environ,
        universal_newlines=True,
    )

    try:
        _, errs = proc.communicate(timeout=timeout)
    except TimeoutExpired:
        proc.kill()
        return 1
    except Exception as e:
        print(command)
        print(e)


    return proc.returncode


def make_filelist(source_dir: str, filelist_path: str) -> None:
    os.chdir(source_dir)
    subprocess.run('find -type f -not -path "./.*" > ' + filelist_path, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, shell=True)


def remove_file(src_path: str) -> None:
    if os.path.exists(src_path):
        os.remove(src_path)


def delete_file_or_dir(path: str) -> None:
    """Delete file or directory tree"""
    if os.path.isfile(path):
        os.remove(path)

    if os.path.isdir(path):
        shutil.rmtree(path)


def extract_nested_zip(zipped_file: str, to_folder: str) -> None:
    """Extract nested zipped files to specified folder"""
    with zipfile.ZipFile(zipped_file, "r") as zfile:
        zfile.extractall(path=to_folder)

    for root, dirs, files in os.walk(to_folder):
        for filename in files:
            if re.search(r"\.zip$", filename):
                filespec = os.path.join(root, filename)
                extract_nested_zip(filespec, root)


