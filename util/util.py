#!/usr/bin/env python3
import csv
import signal
import subprocess
import os


def run_shell_command(command, cwd=None, timeout=30, shell=False):
    """
    Run the given command as a subprocess

    Args:
        command: The child process that should be executed
        cwd: Sets the current directory before the child is executed
        timeout: The number of seconds to wait before timing out the subprocess
        shell: If true, the command will be executed through the shell.
    Returns:
        Tuple[subprocess return code, strings written to stdout, strings written to stderr]
    """
    os.environ['PYTHONUNBUFFERED'] = "1"
    stdout = []
    stderr = []

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

    return proc.returncode, stdout, stderr


def run_siegfried(source_dir: str, target_dir: str, tsv_path: str, zipped=False):
    """
    Generate tsv file with info about file types by running

    Args:
        source_dir: the directory containing the files to be checked
        target_dir: The target directory where the csv file will be saved
        tsv_path: The target path for tsv file
        zipped: nothing...
    """
    if not zipped:
        print('\nIdentifying file types...')

    csv_path = os.path.join(target_dir, 'siegfried.csv')
    os.chdir(source_dir)
    subprocess.run(
        'sf -z -csv * > ' + csv_path,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        shell=True

    )

    with open(csv_path, 'r') as csvin, open(tsv_path, 'w') as tsvout:
        csvin = csv.reader(csvin)
        tsvout = csv.writer(tsvout, delimiter='\t')
        for row in csvin:
            tsvout.writerow(row)

    if os.path.exists(csv_path):
        os.remove(csv_path)
