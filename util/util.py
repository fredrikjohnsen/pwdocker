from __future__ import annotations
import re
import shutil
import subprocess
import os
import signal
import zipfile
import psutil
import time
from config import cfg
from pathlib import Path
from rich.console import Console

console = Console()


def run_shell_cmd(command, cwd=None, timeout=None,
                  shell=False) -> tuple[int, str, str]:
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
        return 1, '', e

    return proc.returncode, out, err


def make_filelist(source_dir, filelist_path):
    """Create a file list from source directory using Siegfried or simple listing"""
    try:
        # Debug information
        console.print(f"make_filelist called with:", style="bold blue")
        console.print(f"  source_dir: {source_dir}", style="blue")
        console.print(f"  filelist_path: {filelist_path}", style="blue")
        
        # Ensure the directory for filelist exists
        filelist_dir = os.path.dirname(filelist_path)
        Path(filelist_dir).mkdir(parents=True, exist_ok=True)
        console.print(f"Created directory: {filelist_dir}", style="green")
        
        if not os.path.exists(source_dir):
            raise FileNotFoundError(f"Source directory does not exist: {source_dir}")
        
        # Check if directory has any files
        file_count = 0
        for root, dirs, files in os.walk(source_dir):
            file_count += len(files)
            
        console.print(f"Found {file_count} files in {source_dir}", style="bold green")
        
        if file_count == 0:
            console.print(f"No files found in {source_dir}", style="bold yellow")
            # Create empty file list with proper header
            with open(filelist_path, 'w', encoding='utf-8') as f:
                f.write("filename,filesize,modified,errors\n")
            console.print(f"Created empty filelist: {filelist_path}", style="yellow")
            return
        
        # Use Siegfried if available and configured
        use_siegfried = cfg.get('use_siegfried', True)
        siegfried_available = shutil.which('sf') is not None
        
        console.print(f"Siegfried config: use={use_siegfried}, available={siegfried_available}", style="blue")
        
        if use_siegfried and siegfried_available:
            console.print("Using Siegfried for file identification...", style="bold blue")
            try:
                # Use Siegfried to create detailed file list
                cmd = ['sf', '-csv', source_dir]
                console.print(f"Running command: {' '.join(cmd)}", style="blue")
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                
                if result.returncode == 0 and result.stdout.strip():
                    with open(filelist_path, 'w', encoding='utf-8') as f:
                        f.write(result.stdout)
                    console.print(f"Siegfried file list created: {filelist_path}", style="bold green")
                    
                    # Verify the file was created and has content
                    if os.path.exists(filelist_path) and os.path.getsize(filelist_path) > 0:
                        console.print(f"File verified: size={os.path.getsize(filelist_path)} bytes", style="green")
                        return
                    else:
                        console.print("Siegfried output file is empty, falling back to simple list", style="yellow")
                else:
                    console.print(f"Siegfried failed (exit code {result.returncode}), creating simple file list", style="bold yellow")
                    if result.stderr:
                        console.print(f"Siegfried error: {result.stderr}", style="red")
            except subprocess.TimeoutExpired:
                console.print("Siegfried timed out, creating simple file list", style="bold yellow")
            except Exception as e:
                console.print(f"Siegfried error: {e}, creating simple file list", style="bold yellow")
        
        # Fallback to simple file list
        console.print("Creating simple file list...", style="bold yellow")
        create_simple_filelist(source_dir, filelist_path)
        
        # Verify the file was created
        if os.path.exists(filelist_path):
            file_size = os.path.getsize(filelist_path)
            console.print(f"Simple file list created: {filelist_path} ({file_size} bytes)", style="bold green")
        else:
            raise FileNotFoundError(f"Failed to create filelist: {filelist_path}")
            
    except Exception as e:
        console.print(f"Error creating file list: {e}", style="bold red")
        console.print(f"Source dir exists: {os.path.exists(source_dir)}", style="red")
        console.print(f"Filelist dir exists: {os.path.exists(os.path.dirname(filelist_path))}", style="red")
        raise


def create_simple_filelist(source_dir, filelist_path):
    """Create a simple file list without file identification"""
    try:
        console.print(f"Creating simple filelist at: {filelist_path}", style="blue")
        
        with open(filelist_path, 'w', encoding='utf-8') as f:
            # Write CSV header compatible with Siegfried format
            f.write("filename,filesize,modified,errors\n")
            
            file_count = 0
            for root, dirs, files in os.walk(source_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        # Get relative path from source directory
                        rel_path = os.path.relpath(file_path, source_dir)
                        file_size = os.path.getsize(file_path)
                        modified = int(os.path.getmtime(file_path))
                        
                        # Escape quotes in filename
                        rel_path = rel_path.replace('"', '""')
                        f.write(f'"{rel_path}",{file_size},{modified},\n')
                        file_count += 1
                        
                    except (OSError, IOError) as e:
                        console.print(f"Warning: Could not process file {file_path}: {e}", style="bold yellow")
                        # Still add the file with error info
                        rel_path = os.path.relpath(file_path, source_dir).replace('"', '""')
                        f.write(f'"{rel_path}",0,0,"Error: {str(e)}"\n')
                        continue
        
        console.print(f"Simple file list created with {file_count} files", style="bold green")
        
    except Exception as e:
        console.print(f"Error creating simple file list: {e}", style="bold red")
        raise


def remove_file(file_path):
    """Safely remove a file"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        console.print(f"Warning: Could not remove file {file_path}: {e}", style="bold yellow")


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


def start_uno_server():
    """Start LibreOffice UNO server if needed"""
    try:
        # Check if LibreOffice UNO server is needed for conversions
        if shutil.which('libreoffice'):
            console.print("LibreOffice found, UNO server available for document conversion", style="bold blue")
        else:
            console.print("LibreOffice not found, document conversion may be limited", style="bold yellow")
    except Exception as e:
        console.print(f"Warning: Error checking LibreOffice: {e}", style="bold yellow")


def uno_server_running():
    for process in psutil.process_iter():
        if process.name() in ['soffice', 'soffice.bin']:
            return True

    return False
