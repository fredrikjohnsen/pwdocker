from __future__ import annotations
import re
import shutil
import subprocess
import os
import signal
import zipfile
from config import cfg


def run_shell_command(command, cwd=None, timeout=None, shell=False) -> tuple[int, str, str]:
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

    # Make calls from subprocess timeout before main subprocess
    if not timeout:
        timeout = cfg['timeout'] - 1

    try:
        proc = subprocess.Popen(
            command,
            cwd=cwd,
            shell=shell,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=os.environ,
            universal_newlines=True,
            start_new_session=True,
        )
        out, err = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        return 1, 'timeout', None
    except Exception as e:
        print(command)
        print(e)

    return proc.returncode, out, err


def make_filelist(source_dir: str, filelist_path: str) -> None:
    os.chdir(source_dir)
    cmd = 'find -type f -not -path "./.*" | cut -c 3- > "' + filelist_path + '"'
    subprocess.run( cmd,
                   stderr=subprocess.DEVNULL,
                   stdout=subprocess.DEVNULL,
                   shell=True)


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
