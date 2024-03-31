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
import typer

from rich.console import Console
import petl as etl

from storage import ConvertStorage, StorageSqliteImpl
from util import make_filelist, remove_file, File
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
    limit: int = None,
    reconvert: bool = False,
    identify_only: bool = False,
    filecheck: bool = False,
    keep_temp: bool = False
) -> None:
    """
    Convert all files in SOURCE folder

    --db-path: Database path. If not set, it uses default DEST + .db

    --filecheck: Check if files in source match files in database

    --status:  Filter on status: accepted, converted, deleted, failed,\n
    ..         protected, skipped, timeout

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

    with StorageSqliteImpl(db_path) as file_storage:
        conv_before, conv_now, total = \
            convert_folder(source, dest, debug, orig_ext, file_storage, '',
                           first_run, None, mime, puid, status, limit,
                           reconvert, identify_only, filecheck, timestamp,
                           keep_temp)

        if total is False:
            msg = "User terminated"
            color = "bold red"
        else:
            # check conversion result
            msg, color = get_conversion_result(conv_before, conv_now, total)

        console.print(msg, style=color)


def convert_folder(
    source_dir: str,
    dest_dir: str,
    debug: bool,
    orig_ext: bool,
    file_storage: ConvertStorage,
    unpacked_path: str,
    first_run: bool,
    source_id: int = None,
    mime: str = None,
    puid: str = None,
    status: str = None,
    limit: int = None,
    reconvert: bool = False,
    identify_only: bool = False,
    filecheck: bool = False,
    timestamp: datetime.datetime = None,
    keep_temp: bool = False
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
            return 0, 0, False

    if not unpacked_path:
        console.print("Converting files..", style="bold cyan")

    # Get table and number of converted files
    if is_new_batch:
        table = file_storage.get_new_rows(limit)
        files_conv_count = 0
    else:
        written_row_count = file_storage.get_row_count(mime, status)
        table = file_storage.get_rows(mime, puid, status, limit,
                                      reconvert or identify_only,
                                      timestamp)
        files_conv_count = written_row_count - etl.nrows(table)
        if files_conv_count > 0:
            console.print(f"({files_conv_count}/{written_row_count}) files "
                          "have already been converted", style="bold cyan")
        # print the files in this directory that have already been converted
        if etl.nrows(table) == 0:
            return files_conv_count, 0, written_row_count

    file_count = etl.nrows(table)

    if not unpacked_path and input(f"Converts {etl.nrows(table)} files. "
                                   "Continue? [y/n] ") != 'y':
        return 0, 0, False

    # loop through all files and run conversion:
    t0 = time.time()
    # unpacked files are added to and converted in main loop
    if not unpacked_path:
        table.row_count = 0
        i = 0
        while etl.nrows(table):
            row = etl.dicts(table)[0]
            file_count = table.row_count + etl.nrows(table)
            i += 1
            table.row_count += 1

            if (
                reconvert and row['dest_path'] and
                os.path.isfile(Path(dest_dir, row['dest_path']))
            ):
                file_storage.delete_descendants(row['id'])

            print(end='\x1b[2K')  # clear line
            print(f"\r({str(table.row_count)}/{str(file_count)}): "
                  f"{row['path'][0:100]}", end=" ", flush=True)

            unidentify = reconvert or identify_only
            source_file = File(row, pwconv_path, file_storage, unidentify)
            norm_path = source_file.convert(source_dir, dest_dir, orig_ext,
                                            debug, identify_only)

            # If conversion failed
            if norm_path is False:
                console.print('  ' + source_file.status, style="bold red")
                file_count -= 1
            elif norm_path:
                dest_path = Path(dest_dir, norm_path)

                if keep_temp or source_file.source_id is None or source_file.kept:
                    source_id = source_file.id
                else:
                    source_id = source_file.source_id

                if os.path.isdir(dest_path):
                    unpacked_count = sum([len(files) for r, d, files
                                          in os.walk(dest_path)])
                    console.print(f'Unpacked {unpacked_count} files',
                                  style="bold cyan", end=' ')

                    count_before, count_now, total = \
                        convert_folder(dest_dir, dest_dir, debug, orig_ext,
                                       file_storage, norm_path, True,
                                       source_id=source_id, keep_temp=keep_temp)

                    file_count += total
                else:
                    file_storage.add_row({'path': norm_path,
                                          'source_id': source_id})

            source_file.status_ts = datetime.datetime.now()
            if keep_temp or source_file.source_id is None or source_file.kept:
                file_storage.update_row(source_file.__dict__)
            else:
                file_storage.delete_row(source_file.__dict__)

    print(str(round(time.time() - t0, 2)) + ' sek')

    converted_count = etl.nrows(file_storage.get_converted_rows(mime))

    return files_conv_count, converted_count, file_count


def write_id_file_to_storage(tsv_source_path: str, source_dir: str,
                             file_storage: ConvertStorage, unpacked_path: str,
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


def get_conversion_result(before: int, to_convert: int,
                          total: int) -> tuple[str, str]:
    print('before', before)
    print('to_convert', to_convert)
    print('total', total)
    if before == total:
        return "All files converted previously.", "bold cyan"
    if total - before == to_convert:
        return "All files converted successfully.", "bold green"
    else:
        return ("Not all files were converted. See the db table for details.",
                "bold cyan")


def check_files(source_dir, unpacked_path, file_storage):
    """ Check if files in database match files on disk """

    files_count = sum([len(files) for r, d, files in os.walk(source_dir)])
    total_row_count = file_storage.get_row_count(original=True)

    if not unpacked_path and files_count != total_row_count:
        console.print(f"Row count: {str(total_row_count)}", style="red")
        console.print(f"File count: {str(files_count)}", style="red")
        db_files = []
        table = file_storage.get_all_rows('', None)
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
