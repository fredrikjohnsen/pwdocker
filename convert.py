# Copyright(C) 2022 Morten Eek

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations
print("Import 1: annotations")
import os
print("Import 2: os")
import shutil
print("Import 3: shutil")
import datetime
print("Import 4: datetime")
import time
print("Import 5: time")
import textwrap
print("Import 6: textwrap")
from pathlib import Path
print("Import 7: pathlib")
from multiprocessing import Pool, Manager
print("Import 8: multiprocessing")
import typer
print("Import 9: typer")

from rich.console import Console
print("Import 10: rich")
import petl as etl
print("Import 11: petl")
from dotenv import load_dotenv
from storage import Storage
print("Import 12: storage")
from file import File
print("Import 13: file")
from util import make_filelist, remove_file, start_uno_server
print("Import 14: util")
from config import cfg, converters
print("Import 15: config")

print("All imports successful")
console = Console()
print("Console created")

# Move these lines up here, before any @app.command() decorators
pwconv_path = Path(__file__).parent.resolve()
print(f"PWConv path: {pwconv_path}")
os.chdir(pwconv_path)
print(f"Changed to directory: {os.getcwd()}")
app = typer.Typer(rich_markup_mode="markdown")
print("Typer app created")
load_dotenv()

def remove_fields(table, *args):
    """Remove fields from petl table"""
    for field in args:
        if field in etl.fieldnames(table):
            table = etl.cutout(table, field)
    return table


def add_fields(table, *args):
    """Add fields to petl table"""
    for field in args:
        if field not in etl.fieldnames(table):
            table = etl.addfield(table, field, None)
    return table


# handle raised errors
def handle_error(error):
    print(error, flush=True)


@app.command()
def convert(
    source: str,
    dest: str = typer.Option(default=None, help="Path to destination folder"),
    orig_ext: bool = typer.Option(default=cfg['keep-original-ext'],
                                  help="Keep original extension"),
    debug: bool = typer.Option(default=cfg['debug'], help="Turn on debug"),
    mime: str = typer.Option(default=None, help="Filter on mime-type"),
    puid: str = typer.Option(default=None,
                             help="Filter on PRONOM Unique Identifier"),
    ext: str = typer.Option(default=None, help="Filter on file extension"),
    status: str = typer.Option(
        default=None,
        help="Filter on conversion status"
    ),
    db: str = typer.Option(default=None, help="Name of MySQL base"),
    reconvert: bool = typer.Option(default=False, help="Reconvert files"),
    identify_only: bool = typer.Option(
        default=False, help="Don't convert, only identify files"
    ),
    filecheck: bool = typer.Option(
        default=False, help="Check if files in source match files in database"
    ),
    set_source_ext: bool = typer.Option(
        default=False, help="Check if files in source match files in database"
    ),
    from_path: str = typer.Option(
        default=None,
        help="Convert files where path ≥ this value"
    ),
    to_path: str = typer.Option(
        default=None,
        help="Convert files where path < this value"
    ),
    multi: bool = typer.Option(
        default=False,
        help="Use multiprocessing to convert each subfolder in its own process"
    ),
    retry: bool = typer.Option(
        default=False,
        help="Try to convert files where conversion previously failed"
    ),
    keep_originals: bool = typer.Option(
        default=cfg['keep-original-files'],
        help="Keep original files"
    )
) -> None:
    try:
        console.print("Starting conversion process...", style="bold cyan")

        if dest is None:
            dest = source

        # Convert to absolute paths and ensure they exist
        source = os.path.abspath(source)
        dest = os.path.abspath(dest)
        
        console.print(f"Source: {source}", style="bold cyan")
        console.print(f"Destination: {dest}", style="bold cyan")
        
        if not os.path.exists(source):
            console.print(f"Source directory does not exist: {source}", style="bold red")
            return False  # This should probably raise an exception or exit with error code
            
        Path(dest).mkdir(parents=True, exist_ok=True)
        timestamp = datetime.datetime.now()

        # Add more debug output
        console.print(f"Setting up database connection...", style="bold cyan")
        
        # Handle database connection - use MySQL if environment variables are set
        if not db:
            if os.getenv('DB_HOST'):
                # Use MySQL from environment
                console.print("Using MySQL database from environment variables", style="bold cyan")
                db = 'mysql'
            else:
                # Use SQLite file
                console.print(f"Using SQLite database at {os.path.join(dest, 'convert.db')}", style="bold cyan")
                db = os.path.join(dest, 'convert.db')

        try:
            with Storage(db) as store:
                console.print("Database connection established", style="bold green")
                
                # Create filelist in a writable location
                data_dir = os.path.join(os.getcwd(), 'data')
                Path(data_dir).mkdir(parents=True, exist_ok=True)
                console.print(f"Data directory: {data_dir}", style="bold cyan")
                
                # Use a consistent filename based on source path hash to avoid conflicts
                import hashlib
                source_hash = hashlib.md5(source.encode()).hexdigest()[:8]
                filelist_path = os.path.join(data_dir, f'filelist-{source_hash}.txt')
                console.print(f"File list path: {filelist_path}", style="bold cyan")
                
                # Fix the logic here - is_new_batch should be True if file does NOT exist
                is_new_batch = not os.path.isfile(filelist_path)
                first_run = store.get_row_count() == 0
                console.print(f"First run: {first_run}, New batch: {is_new_batch}", style="bold cyan")
                
                # Initialize status variable
                if not status:
                    status = 'new'  # Default status
                
                # Create filelist if it's a first run OR if we don't have a filelist yet
                if first_run or is_new_batch:
                    console.print(f"Creating file list from: {source}", style="bold cyan")
                    try:
                        make_filelist(source, filelist_path)
                        
                        console.print(f"Writing file list to database...", style="bold cyan")
                        write_id_file_to_storage(filelist_path, source, store, '')
                        status = 'new'  # Override status after creating new files
                    except Exception as e:
                        console.print(f"Error creating file list: {e}", style="bold red")
                        return False

                conds, params = store.get_conds(mime=mime, puid=puid, status=status,
                                                reconvert=(reconvert or identify_only),
                                                from_path=from_path, to_path=to_path,
                                                timestamp=timestamp, ext=ext, retry=retry)

                count_remains = store.get_row_count(conds, params)
                
                if count_remains == 0:
                    console.print("No files to convert found.", style="bold yellow")
                    return False  # Same issue here
                    
                m = Manager()
                count = {
                    'remains': m.Value('i', count_remains),
                    'finished': m.Value('i', 0)
                }

                start_uno_server()
                msg = f"Converts {count_remains} files. "
                if dest == source and keep_originals is False:
                    msg += ("You have chosen to convert files within source folder "
                            "and not keep original files. This deletes original files "
                            "that are converted. Consider backing up folder before "
                            "proceeding to safeguard against data loss.")
                elif dest == source:
                    msg += "Files marked with `kept: false` will be deleted. "

                msg += "Continue? [y/n] "

                # Skip confirmation in Docker environment or if DEBUG is false
                if os.getenv('DOCKER_ENV') or not debug:
                    console.print("Auto-continuing in Docker environment...", style="bold green")
                else:
                    if input(textwrap.dedent(msg)) != 'y':
                        return False

                warning = ""
                for converter in converters.values():
                    if converter.get('command') and 'unoconv2x' in converter['command']:
                        warning += "unoconv2x is deprecated and will be removed in a "
                        warning += "coming update. Use unoconvert instead. "
                        warning += "Continue? [y/n]"
                if warning:
                    if input(warning) != 'y':
                        return False

                if filecheck:
                    res = check_files(source, store)
                    if res == 'cancelled':
                        return False

                console.print("Converting files..", style="bold cyan")

                pool = Pool()
                t0 = time.time()

                try:
                    if multi:
                        dirs = store.get_subfolders(conds, params)
                        console.print(f"Found {len(dirs)} subdirectories to process", style="bold cyan")
                        for dir in dirs:
                            dir = Path(dir).name
                            args = (source, dest, debug, orig_ext, db, dir, True,
                                    mime, puid, ext, status, reconvert, retry,
                                    identify_only, filecheck, timestamp, set_source_ext,
                                    from_path, to_path, count, keep_originals)
                            pool.apply_async(convert_folder, args=args, error_callback=handle_error)
                    else:
                        console.print("Processing single folder", style="bold cyan")
                        result = convert_folder(source, dest, debug, orig_ext, db, '', True,
                                    mime, puid, ext, status, reconvert, retry,
                                    identify_only, filecheck, timestamp, set_source_ext,
                                    from_path, to_path, count, keep_originals)
                        console.print(f"Folder processing result: {result}", style="bold cyan")

                    console.print("Waiting for all processes to complete...", style="bold cyan")
                    pool.close()
                    pool.join()
                    console.print("All processes completed", style="bold green")
                    
                    console.print("Starting result summary...", style="bold cyan")
                    duration = str(datetime.timedelta(seconds=round(time.time() - t0)))
                    console.print('\nConversion finished in ' + duration)
                    
                    console.print("Querying accepted files...", style="bold cyan")
                    conds, params = store.get_conds(finished=True, status='accepted',
                                                timestamp=timestamp)
                    count_accepted = store.get_row_count(conds, params)
                    console.print(f"Found {count_accepted} accepted files", style="bold cyan")
                    
                    conds, params = store.get_conds(finished=True, status='skipped',
                                                timestamp=timestamp)
                    count_skipped = store.get_row_count(conds, params)
                    if count_skipped:
                        console.print(f"{count_skipped} files skipped",
                                      style="bold orange1")
                    conds, params = store.get_conds(finished=True, status='removed',
                                                timestamp=timestamp)
                    count_removed = store.get_row_count(conds, params)
                    if count_removed:
                        console.print(f"{count_removed} files removed",
                                      style="bold orange1")

                    conds, params = store.get_conds(finished=True, status='failed',
                                                timestamp=timestamp)
                    count_failed = store.get_row_count(conds, params)
                    if count_failed:
                        console.print(f"{count_failed} files failed",
                                      style="bold red")
                    console.print(f"See database {db} for details")
                except Exception as e:
                    console.print(f"Error during conversion process: {e}", style="bold red")
                    pool.terminate()  # Make sure to terminate the pool on error
                    pool.join()
                    return False
        except Exception as e:
            console.print(f"Database connection error: {e}", style="bold red")
            return False
    except KeyboardInterrupt:
        console.print("\nConversion interrupted by user", style="bold yellow")
        if 'pool' in locals():
            pool.terminate()
            pool.join()
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"Unexpected error in conversion: {e}", style="bold red")
        import traceback
        traceback.print_exc()
        if 'pool' in locals():
            pool.terminate()
            pool.join()
        raise typer.Exit(1)


def convert_folder(
    source_dir: str,
    dest_dir: str,
    debug: bool,
    orig_ext: bool,
    db: str,
    subpath: str,
    multi: bool,
    mime: str,
    puid: str,
    ext: str,
    status: str,
    reconvert: bool,
    retry: bool,
    identify_only: bool,
    filecheck: bool,
    timestamp: datetime.datetime,
    set_source_ext: bool,
    from_path: str,
    to_path: str,
    count: dict,
    keep_originals: bool
) -> tuple[str, str]:
    """Convert all files in folder"""
    
    try:
        # Get all the files to process first with a single database connection
        with Storage(db) as store:
            if reconvert:
                conds, params = store.get_conds(
                    mime=mime, puid=puid, status=status, subpath=subpath,
                    reconvert=(reconvert or identify_only), ext=ext,
                    from_path=from_path, to_path=to_path, timestamp=timestamp,
                    retry=retry
                )
                store.update_status(conds, params, 'new')
            else:
                conds, params = store.get_conds(
                    mime=mime, puid=puid, status=status, subpath=subpath, ext=ext,
                    from_path=from_path, to_path=to_path, timestamp=timestamp,
                    reconvert=identify_only, retry=retry
                )
                
            # Get all rows at once to avoid keeping the connection open for too long
            table = store.get_rows(conds, params)
            all_rows = list(etl.dicts(table))
        
        # Process files in smaller batches, creating a new connection for each batch
        batch_size = 20  # Smaller batch size to reduce connection time
        total_rows = len(all_rows)
        
        for offset in range(0, total_rows, batch_size):
            batch_rows = all_rows[offset:offset + batch_size]
            
            # Process this batch of files
            for row in batch_rows:
                # Open a new connection for each file operation that needs database access
                with Storage(db) as store:
                    process_single_file(row, source_dir, dest_dir, orig_ext, debug, 
                                      set_source_ext, identify_only, keep_originals,
                                      store, count, reconvert, pwconv_path)
                
    except Exception as e:
        console.print(f"Database error in convert_folder: {e}", style="bold red")
        raise


def process_single_file(row, source_dir, dest_dir, orig_ext, debug, 
                       set_source_ext, identify_only, keep_originals,
                       store, count, reconvert, pwconv_path):
    """Process a single file conversion"""
    try:
        count['finished'].value += 1
        
        if row['source_id'] is None:
            pass  # Handle source file logic
        
        n = count['remains'].value
        new_percent = round((1 - n/(n + count['finished'].value)) * 100)
        
        # Use safe encoding for file path display
        file_path = row.get('path', 'unknown')
        try:
            display_path = file_path.encode('utf-8', errors='replace').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError):
            display_path = repr(file_path)  # Fallback to repr for problematic paths
            
        print(end='\x1b[2K')  # clear line
        print(f"\r{new_percent}% | "
              f"{display_path[0:100]}", end=" ", flush=True)

        unidentify = reconvert or identify_only
         
        # Handle encoding issues when creating File object
        try:

            src_file = File(row, pwconv_path, unidentify)
            console.print(f"File object created successfully", style="dim")
        except Exception as e:
            error_msg = f"Error creating File object for {display_path}: {e}"
            console.print(error_msg, style="bold red")
            
            # Update file status to failed with detailed error
            if store and hasattr(store, 'update_file_status'):
                try:
                    store.update_file_status(row.get('id'), 'failed', str(e))
                except Exception as db_err:
                    console.print(f"Database update error: {db_err}", style="bold red")
            return
            
        try:
            console.print(f"Starting conversion for: {display_path}", style="dim")
            norm = src_file.convert(source_dir, dest_dir, orig_ext,
                                  debug, set_source_ext, identify_only,
                                  keep_originals)
            console.print(f"Conversion completed for: {display_path}", style="dim")
        except Exception as e:
            error_msg = f"Error during conversion for {display_path}: {e}"
            console.print(error_msg, style="bold red")
            
            # Update file status to failed
            if store and hasattr(store, 'update_file_status'):
                try:
                    store.update_file_status(row.get('id'), 'failed', str(e))
                except Exception as db_err:
                    console.print(f"Database update error: {db_err}", style="bold red")
            return

        # Handle conversion results
        if norm is False:
            if src_file.status != 'accepted':
                console.print(f"  {src_file.status}", style="bold red")
        elif type(norm) is str:
            handle_unpacked_files(norm, dest_dir, store, src_file, count)
        else:
            handle_converted_file(norm, store)

        # Update source file status
        try:
            src_file.status_ts = datetime.datetime.now()
            
            # Ensure store is still valid before updating
            if store and hasattr(store, 'update_row'):
                store.update_row(src_file.__dict__)
        except Exception as db_err:
            console.print(f"Database update error for {display_path}: {db_err}", style="bold red")
        
        count['remains'].value -= 1
        
    except UnicodeError as e:
        error_msg = f"Unicode/encoding error processing file {row.get('path', 'unknown')}: {e}"
        console.print(error_msg, style="bold red")
        # Update file status to failed
        if store and hasattr(store, 'update_file_status'):
            try:
                store.update_file_status(row.get('id'), 'failed', f'Encoding error: {str(e)}')
            except:
                pass
                
    except Exception as e:
        error_msg = f"Error processing file {row.get('path', 'unknown')}: {e}"
        console.print(error_msg, style="bold red")
        
        # Print full traceback for debugging
        if debug:
            import traceback
            traceback.print_exc()
        
        # Update file status to failed - only if store is available
        if store and hasattr(store, 'update_file_status'):
            try:
                store.update_file_status(row.get('id'), 'failed', str(e))
            except:
                pass  # Avoid cascading database errors


def write_id_file_to_storage(tsv_source_path: str, source_dir: str,
                             store: Storage, unpacked_path: str,
                             source_id: int = None) -> int:
    """Write file list to database storage"""
    try:
        console.print(f"Reading file list from: {tsv_source_path}", style="bold blue")
        
        if not os.path.exists(tsv_source_path):
            raise FileNotFoundError(f"File list not found: {tsv_source_path}")
        
        # Read the file list with proper encoding handling
        try:
            # Try as Siegfried CSV first with UTF-8 encoding
            table = etl.fromcsv(tsv_source_path, encoding='utf-8')
        except UnicodeDecodeError:
            try:
                # Fallback to latin-1 encoding
                table = etl.fromcsv(tsv_source_path, encoding='latin-1')
            except:
                try:
                    # Last resort - try with error handling
                    table = etl.fromcsv(tsv_source_path, encoding='utf-8', errors='replace')
                except:
                    # Fallback to simple text format
                    with open(tsv_source_path, 'r', encoding='utf-8', errors='replace') as f:
                        lines = f.readlines()
                    table = etl.fromtext(tsv_source_path, header=['filename'], strip="\n", encoding='utf-8', errors='replace')
        
        # Rename columns to standard format
        table = etl.rename(
            table,
            {
                'filename': 'path',
                'filesize': 'size',
                'Content_Type': 'mime',
                'Version': 'version',
                'id': 'puid',  # PRONOM ID from Siegfried
            },
            strict=False,
        )
        
        # Filter out empty paths and handle encoding issues in paths
        def safe_path_filter(rec):
            path = getattr(rec, 'path', '')
            if not path:
                return False
            try:
                # Test if path can be properly encoded/decoded
                path.encode('utf-8').decode('utf-8')
                return True
            except UnicodeError:
                console.print(f"Skipping file with encoding issue: {repr(path)}", style="yellow")
                return False
                
        table = etl.select(table, safe_path_filter)
        
        # Add required fields that may be missing
        table = add_fields(table, 'mime', 'version', 'status', 'puid', 'source_id')
        
        # Remove Siegfried-specific columns that we don't need
        table = remove_fields(table, "namespace", "basis", "warning", "errors", "modified")
        
        # Set default values
        table = etl.update(table, 'status', "new")
        if source_id:
            table = etl.update(table, 'source_id', source_id)
        
        # Handle special cases for file type detection
        # Treat csv (detected from extension only) as plain text:
        table = etl.convert(table, "mime", lambda v, _row: 
                          "text/plain" if getattr(_row, 'puid', '') == "x-fmt/18" else v,
                          pass_row=True)
        
        # Update for missing mime types where PUID is known:
        table = etl.convert(table, "mime", lambda v, _row: 
                          "application/xml" if getattr(_row, 'puid', '') == "fmt/979" else v,
                          pass_row=True)
        
        # Handle unpacked files path adjustment
        if unpacked_path:
            table = etl.convert(table, 'path',
                              lambda v: os.path.join(unpacked_path, v))
        
        # Insert into database
        row_count = store.append_rows(table)
        console.print(f"Inserted {row_count} files into database", style="bold green")
        
        # Clean up the temporary file
        remove_file(tsv_source_path)
        return row_count
        
    except Exception as e:
        console.print(f"Error writing file list to storage: {e}", style="bold red")
        raise


def check_files(source_dir, store):
    """ Check if files in database match files on disk """

    files_count = sum([len(files) for r, d, files in os.walk(source_dir)])
    conds, params = store.get_conds(original=True)
    total_row_count = store.get_row_count(conds, params)

    if files_count != total_row_count:
        console.print(f"Row count: {str(total_row_count)}", style="red")
        console.print(f"File count: {str(files_count)}", style="red")
        db_files = []
        table = store.get_all_rows('')
        for row in etl.dicts(table):
            db_files.append(row['path'])
        print("Following files don't exist in database:")
        extra_files = []
        for r, d, files in os.walk(source_dir):
            for file_ in files:
                path = Path(r, file_)
                commonprefix = os.path.commonprefix([source_dir, path])
                relpath = os.path.relpath(path, commonprefix)
                if relpath not in db_files:
                    extra_files.append({'path': relpath, 'status': 'new'})
                    print('- ' + relpath)

        answ = input(f"Files listed in database doesn't match "
                     "files on disk. Continue? [y]es, [n]o, [a]dd, [d]elete ")
        if answ == 'd':
            for file_ in extra_files:
                Path(source_dir, file_['source_path']).unlink()
            return 'deleted'
        elif answ == 'a':
            table = etl.fromdicts(extra_files)
            store.append_rows(table)
            return 'added'
        elif answ != 'y':
            return 'cancelled'


def handle_unpacked_files(unpacked_path, dest_dir, store, src_file, count):
    """Handle files that were unpacked from archives"""
    try:
        # Add unpacked files to database
        filelist_path = os.path.join('/tmp', f'unpacked-{src_file.id}.txt')
        make_filelist(unpacked_path, filelist_path)
        
        row_count = write_id_file_to_storage(filelist_path, unpacked_path, store, 
                                           unpacked_path, src_file.id)
        
        count['remains'].value += row_count
        console.print(f"Added {row_count} unpacked files to queue", style="bold blue")
        
    except Exception as e:
        console.print(f"Error handling unpacked files: {e}", style="bold red")


def handle_converted_file(converted_file, store):
    """Handle successfully converted file"""
    try:
        # Update database with conversion result
        if hasattr(converted_file, '__dict__'):
            store.update_row(converted_file.__dict__)
        else:
            console.print("Conversion completed successfully", style="bold green")
            
    except Exception as e:
        console.print(f"Error updating converted file: {e}", style="bold red")


if __name__ == "__main__":
    app()
