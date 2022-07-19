import signal
import subprocess
import os

from typing import List

from bin.pdf2pdfa import pdf2pdfa


def run_shell_command(command, cwd=None, timeout=30, shell=False):
    """Run shell command"""
    os.environ['PYTHONUNBUFFERED'] = "1"
    stdout = []
    stderr = []
    mix = []  # TODO: Fjern denne mm

    # sys.stdout.flush()

    proc = subprocess.Popen(
        command,
        cwd=cwd,
        shell=shell,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.kill(proc.pid, signal.SIGINT)

    for line in proc.stdout:
        stdout.append(line.rstrip())

    for line in proc.stderr:
        stderr.append(line.rstrip())

    return proc.returncode, stdout, stderr, mix


def remove_file(file_path: str):
    if os.path.isfile(file_path):
        os.remove(file_path)


def is_conversion_success(status_code: int, file_path: str):
    return os.path.exists(file_path) and status_code == 0


def remove_tmp_and_exit(status_code: int, file_path: str):
    remove_file(file_path)
    if status_code != 0:
        return 1


def run_command_and_convert_to_pdfa(command: List[str], tmp_path: str, target_path: str):
    initial_convert_status_code = run_shell_command(command)[0]
    if not is_conversion_success(initial_convert_status_code, tmp_path):
        remove_tmp_and_exit(initial_convert_status_code, tmp_path)

    status_code = pdf2pdfa(tmp_path, target_path)
    if not is_conversion_success(status_code, target_path):
        remove_tmp_and_exit(status_code, tmp_path)

    remove_file(tmp_path)
    return 0
