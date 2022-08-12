import csv
import re
import shutil
import signal
import subprocess
import os
import zipfile


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

    remove_file(csv_path)


def remove_file(src_path: str):
    if os.path.exists(src_path):
        os.remove(src_path)


def delete_file_or_dir(path: str):
    """Delete file or directory tree"""
    if os.path.isfile(path):
        os.remove(path)

    if os.path.isdir(path):
        shutil.rmtree(path)


def extract_nested_zip(zipped_file: str, to_folder: str):
    """Extract nested zipped files to specified folder"""
    with zipfile.ZipFile(zipped_file, 'r') as zfile:
        zfile.extractall(path=to_folder)

    for root, dirs, files in os.walk(to_folder):
        for filename in files:
            if re.search(r'\.zip$', filename):
                filespec = os.path.join(root, filename)
                extract_nested_zip(filespec, root)


def get_property_defaults(properties, overwrites):
    if not overwrites:
        return properties

    return _merge_dicts(dict(properties), dict(overwrites))


def _merge_dicts(properties, overwrite_with):
    if isinstance(overwrite_with, dict):
        for k, v in overwrite_with.items():
            if k in properties:
                properties[k] = _merge_dicts(properties.get(k), v)
        return properties
    else:
        return overwrite_with
