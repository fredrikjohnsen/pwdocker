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
import os
import shutil
import datetime
import time
from pathlib import Path
import mimetypes
import typer

from rich.console import Console
import petl as etl

from storage import Storage
from file import File
from util import make_filelist, remove_file
from config import cfg

console = Console()
pwconv_path = Path(__file__).parent.resolve()
os.chdir(pwconv_path)


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


def convert(
    source: str,
    dest: str,
    orig_ext: bool = cfg['keep-original-ext'],
    debug: bool = cfg['debug'],
    mime: str = None,
    puid: str = None,
    status: str = None,
    db_path: str = None,
    reconvert: bool = False,
    identify_only: bool = False,
    filecheck: bool = False,
    set_source_ext: bool = False,
    from_path: str = None,
    to_path: str = None
) -> None:
    """
    Convert all files in SOURCE folder

    --db-path:   Database path. If not set, it uses default DEST + .db

    --filecheck: Check if files in source match files in database

    --status:    Filter on status: accepted, converted, deleted, failed,\n
    ..           protected, skipped, timeout

    --from-path: Convert files where path is larger than or the same as this value

    --to-path:   Convert files where path is smaller than this value

    """

    Path(dest).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now()

    if os.path.isdir('/tmp/convert'):
        shutil.rmtree('/tmp/convert')

    first_run = False

    if db_path and os.path.dirname(db_path) == '':
        console.print("Error: --db-path must refer to an absolute path",
                      style='red')
        return
    if not db_path:
        db_path = dest + '.db'
    if not os.path.isfile(db_path):
        first_run = True

    with Storage(db_path) as file_storage:
        total = convert_folder(source, dest, debug, orig_ext, file_storage, '',
                               first_run, None, mime, puid, status,
                               reconvert, identify_only, filecheck, timestamp,
                               set_source_ext, from_path, to_path)

        if total is False:
            console.print("User terminated", style="bold red")
        else:
            # check conversion result
            failed_count = etl.nrows(file_storage.get_failed_rows(mime))
            skipped_count = etl.nrows(file_storage.get_skipped_rows(mime))
            if failed_count:
                msg = f"{failed_count} conversions failed. See db table."
                console.print(msg, style="bold red")
            if skipped_count:
                msg = f"{skipped_count} files skipped. Se db table."
                console.print(msg, style="bold cyan")
            if not (failed_count or skipped_count):
                console.print("All files converted", style="bold green")


def convert_folder(
    source_dir: str,
    dest_dir: str,
    debug: bool,
    orig_ext: bool,
    file_storage: Storage,
    unpacked_path: str,
    first_run: bool,
    source_id: int = None,
    mime: str = None,
    puid: str = None,
    status: str = None,
    reconvert: bool = False,
    identify_only: bool = False,
    filecheck: bool = False,
    timestamp: datetime.datetime = None,
    set_source_ext: bool = False,
    from_path: str = None,
    to_path: str = None
) -> tuple[str, str]:
    """Convert all files in folder"""

    # Write new files to database
    filelist_dir = os.path.join(dest_dir, unpacked_path)
    filelist_path = filelist_dir.rstrip('/') + '-filelist.txt'
    is_new_batch = os.path.isfile(filelist_path)
    if first_run or is_new_batch:
        if not is_new_batch:
            make_filelist(os.path.join(source_dir, unpacked_path),
                          filelist_path)
        write_id_file_to_storage(filelist_path, source_dir, file_storage,
                                 unpacked_path, source_id=source_id)

    if filecheck:
        res = check_files(source_dir, unpacked_path, file_storage)
        if res == 'cancelled':
            return False

    if not unpacked_path:
        console.print("Converting files..", style="bold cyan")

    # Get table and number of converted files
    if is_new_batch:
        status = 'new'
    written_row_count = file_storage.get_row_count(mime, status)
    table = file_storage.get_rows(mime, puid, status,
                                  reconvert or identify_only,
                                  from_path, to_path, timestamp)
    files_conv_count = written_row_count - etl.nrows(table)
    if not unpacked_path and files_conv_count > 0:
        console.print(f"({files_conv_count}/{written_row_count}) files "
                      "have already been converted", style="bold cyan")
    if etl.nrows(table) == 0:
        return 0

    file_count = etl.nrows(table)

    if not unpacked_path and input(f"Converts {etl.nrows(table)} files. "
                                   "Continue? [y/n] ") != 'y':
        return False

    # loop through all files and run conversion:
    t0 = time.time()
    # unpacked files are added to and converted in main loop
    if not unpacked_path:
        table.row_count = 0
        i = 0
        percent = 0
        nrows = etl.nrows(table)
        while nrows > 0:
            i += 1
            row = etl.dicts(table)[0]
            if row['source_id'] is None:
                table.row_count += 1

            new_percent = round((1 - nrows/(nrows + i)) * 100)
            percent = percent if percent > new_percent else new_percent

            if (
                reconvert and row['dest_path'] and
                os.path.isfile(Path(dest_dir, row['dest_path']))
            ):
                file_storage.delete_descendants(row['id'])

            print(end='\x1b[2K')  # clear line
            print(f"\r{percent}% | "
                  f"{row['path'][0:100]}", end=" ", flush=True)

            unidentify = reconvert or identify_only
            src_file = File(row, pwconv_path, unidentify)
            norm_path = src_file.convert(source_dir, dest_dir, orig_ext,
                                         debug, identify_only)

            if identify_only and set_source_ext:
                mime_ext = mimetypes.guess_extension(src_file.mime)
                new_path = str(Path(src_file.parent, src_file.stem + mime_ext))
                shutil.move(src_file.path, new_path)
                src_file.path = new_path

            # If conversion failed
            if norm_path is False:
                console.print('  ' + src_file.status, style="bold red")
            elif norm_path:
                dest_path = Path(dest_dir, norm_path)

                if src_file.source_id is None or src_file.kept:
                    source_id = src_file.id
                else:
                    source_id = src_file.source_id

                if os.path.isdir(dest_path):
                    unpacked_count = sum([len(files) for r, d, files
                                          in os.walk(dest_path)])
                    console.print(f'Unpacked {unpacked_count} files',
                                  style="bold cyan", end=' ')

                    n = convert_folder(dest_dir, dest_dir, debug, orig_ext,
                                       file_storage, norm_path, True,
                                       source_id=source_id)
                    nrows += n

                else:
                    file_storage.add_row({'path': norm_path, 'status': 'new',
                                          'status_ts': datetime.datetime.now(),
                                          'source_id': source_id})
                    nrows += 1

            src_file.status_ts = datetime.datetime.now()
            if src_file.source_id is None or src_file.kept:
                file_storage.update_row(src_file.__dict__)
            else:
                file_storage.delete_row(src_file.__dict__)
            nrows -= 1

    print(str(round(time.time() - t0, 2)) + ' sek')

    return file_count


def write_id_file_to_storage(tsv_source_path: str, source_dir: str,
                             file_storage: Storage, unpacked_path: str,
                             source_id: int = None) -> int:

    table = etl.fromtext(tsv_source_path, header=['filename'], strip="\n")
    table = etl.rename(
        table,
        {
            'filename': 'path',
            'filesize': 'size',
            'Content_Type': 'mime',
            'Version': 'version',
        },
        strict=False,
    )
    table = etl.select(table, lambda rec: rec.path != "")
    table = add_fields(table, 'mime', 'version', 'status', 'puid', 'source_id')
    # Remove Siegfried generated columns
    table = remove_fields(table, "namespace", "basis", "warning")

    table = etl.update(table, 'status', "new")
    table = etl.update(table, 'source_id', source_id)

    # Treat csv (detected from extension only) as plain text:
    table = etl.convert(table, "mime", lambda v,
                        _row: "text/plain" if _row.id == "x-fmt/18" else v,
                        pass_row=True)

    # Update for missing mime types where id is known:
    table = etl.convert(table, "mime", lambda v,
                        _row: "application/xml" if _row.id == "fmt/979" else v,
                        pass_row=True)

    if unpacked_path:
        table = etl.convert(table, 'path',
                            lambda v: os.path.join(unpacked_path, v))

    file_storage.append_rows(table)
    row_count = etl.nrows(table)
    remove_file(tsv_source_path)
    return row_count


def check_files(source_dir, unpacked_path, file_storage):
    """ Check if files in database match files on disk """

    files_count = sum([len(files) for r, d, files in os.walk(source_dir)])
    total_row_count = file_storage.get_row_count(original=True)

    if not unpacked_path and files_count != total_row_count:
        console.print(f"Row count: {str(total_row_count)}", style="red")
        console.print(f"File count: {str(files_count)}", style="red")
        db_files = []
        table = file_storage.get_all_rows('')
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

        answ = input(f"Files listed in {file_storage.path} doesn't match "
                     "files on disk. Continue? [y]es, [n]o, [a]dd, [d]elete ")
        if answ == 'd':
            for file_ in extra_files:
                Path(source_dir, file_['source_path']).unlink()
            return 'deleted'
        elif answ == 'a':
            table = etl.fromdicts(extra_files)
            file_storage.append_rows(table)
            return 'added'
        elif answ != 'y':
            return 'cancelled'


if __name__ == "__main__":
    typer.run(convert)
