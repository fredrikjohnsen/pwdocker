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
import time
from os.path import relpath
from pathlib import Path
from typing import Dict
import typer

from rich.console import Console
import petl as etl
from petl.io.db import DbView

from storage import ConvertStorage, StorageSqliteImpl
from util import make_filelist, remove_file, File, Result
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
    mime_type: str = None,
    puid: str = None,
    result: str = None,
    db_path: str = None,
    limit: int = None,
    reconvert: bool = False,
    identify_only: bool = False,
    check_files: bool = False
) -> None:
    """
    Convert all files in SOURCE folder

    --db-path: Database path. If not set, it uses default DEST + .db
    --check-files: Check if files in source corresponds to files in database
    """

    Path(dest).mkdir(parents=True, exist_ok=True)

    if os.path.isdir('/tmp/convert'):
        shutil.rmtree('/tmp/convert')

    first_run = False

    if db_path and os.path.dirname(db_path) == '':
        console.print("Error: --db-path must refer to an absolute path", style='red')
        return
    if not db_path:
        db_path = dest + '.db'
    if not os.path.isfile(db_path):
        first_run = True

    with StorageSqliteImpl(db_path) as file_storage:
        conv_before, conv_now, total = \
            convert_folder(source, dest, debug, orig_ext, file_storage, '',
                           first_run, mime_type, puid, result, limit, reconvert,
                           identify_only, check_files)

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
    mime_type: str = None,
    puid: str = None,
    result: str = None,
    limit: int = None,
    reconvert: bool = False,
    identify_only: bool = False,
    check_files: bool = False
) -> tuple[str, str]:
    """Convert all files in folder"""

    filelist_dir = os.path.join(dest_dir, unpacked_path)
    filelist_path = filelist_dir.rstrip('/') + '-filelist.txt'
    is_new_batch = os.path.isfile(filelist_path)
    if first_run or is_new_batch:
        if not is_new_batch:
            make_filelist(os.path.join(source_dir, unpacked_path), filelist_path)
        write_id_file_to_storage(filelist_path, source_dir, file_storage,
                                 unpacked_path)

    written_row_count = file_storage.get_row_count(mime_type, result)
    total_row_count = file_storage.get_row_count(None)

    if check_files:
        files_count = sum([len(files) for r, d, files in os.walk(source_dir)])

        if files_count == 0:
            return "No files to convert. Exiting.", "bold red"

        if not unpacked_path and files_count != total_row_count:
            console.print(f"Row count: {str(total_row_count)}", style="red")
            console.print(f"File count: {str(files_count)}", style="red")
            db_files = []
            table = file_storage.get_all_rows('', None)
            for row in etl.dicts(table):
                db_files.append(row['source_path'])
            print("Following files don't exist in database:")
            extra_files = []
            for r, d, files in os.walk(source_dir):
                for file_ in files:
                    path = Path(r, file_)
                    commonprefix = os.path.commonprefix([source_dir, path])
                    relpath = os.path.relpath(path, commonprefix)
                    if relpath not in db_files:
                        extra_files.append({'source_path': relpath, 'result': 'new'})
                        print('- ' + relpath)

            answ = input(f"Files listed in {file_storage.path} doesn't match "
                         "files on disk. Continue? [y]es, [n]o, [a]dd, [d]elete ")
            if answ == 'd':
                for file_ in extra_files:
                    Path(source_dir, file_['source_path']).unlink()
            elif answ == 'a':
                table = etl.fromdicts(extra_files)
                file_storage.append_rows(table)

            elif answ != 'y':
                return 0, 0, False

    if not unpacked_path:
        console.print("Converting files..", style="bold cyan")

    if first_run:
        files_converted_count = 0
        table = file_storage.get_all_rows(unpacked_path, limit)
    elif is_new_batch:
        table = file_storage.get_new_rows(limit)
        files_converted_count = 0
    else:
        table = file_storage.get_rows(mime_type, puid, result, limit,
                                      reconvert or identify_only)
        files_converted_count = written_row_count - etl.nrows(table)
        if files_converted_count > 0:
            console.print(f"({files_converted_count}/{written_row_count}) files have already "
                          "been converted", style="bold cyan")
        # print the files in this directory that have already been converted
        if etl.nrows(table) == 0:
            return files_converted_count, 0, written_row_count

    file_count = etl.nrows(table)

    if not unpacked_path and input(f"Converts {etl.nrows(table)} files. "
                                   "Continue? [y/n] ") != 'y':
        return 0, 0, False

    # run conversion:
    t0 = time.time()
    # unpacked files are added to and converted in main loop
    if not unpacked_path:
        table.row_count = 0
        for row in etl.dicts(table):
            table.row_count += 1
            if (
                reconvert and row['dest_path'] and
                os.path.isfile(Path(dest_dir, row['dest_path']))
            ):
                Path(dest_dir, row['dest_path']).unlink()

            file_count = convert_file(file_count, file_storage, row, source_dir,
                                      table, dest_dir, debug, orig_ext, reconvert,
                                      identify_only)

    print(str(round(time.time() - t0, 2)) + ' sek')

    converted_count = etl.nrows(file_storage.get_converted_rows(mime_type))

    return files_converted_count, converted_count, file_count 


def convert_file(
    file_count: int,
    file_storage: ConvertStorage,
    row: Dict[str, any],
    source_dir: str,
    table: DbView,
    dest_dir: str,
    debug: bool,
    orig_ext: bool,
    reconvert: bool,
    identify_only: bool
) -> None:
    if row['source_mime_type']:
        # TODO: Why is this necessary?
        row["source_mime_type"] = row["source_mime_type"].split(";")[0]
    print(end='\x1b[2K')  # clear line
    print(f"\r({str(table.row_count)}/{str(file_count)}): "
          f"{row['source_path'][0:100]}", end=" ", flush=True)

    unidentify = reconvert or identify_only
    source_file = File(row, pwconv_path, file_storage, unidentify)
    moved_to_dest_path = Path(dest_dir, row['source_path'])
    Path(moved_to_dest_path.parent).mkdir(parents=True, exist_ok=True)
    normalized, temp_path = source_file.convert(source_dir, dest_dir, orig_ext,
                                                debug, identify_only)
    row['source_mime_type'] = source_file.mime_type
    row['format'] = source_file.format
    row['source_file_size'] = source_file.file_size
    row['version'] = source_file.version
    row['puid'] = source_file.puid

    # path to directory for extracted file
    if normalized['dest_path']:
        dir = os.path.join(source_dir, normalized['dest_path'])

    if not identify_only and os.path.isfile(temp_path):
        os.remove(temp_path)

    if identify_only:
        pass
    elif normalized['result'] == Result.REMOVED:
        if moved_to_dest_path.is_file():
            moved_to_dest_path.unlink()
        row['result'] = normalized['result']
        row['moved_to_target'] = None
        row['dest_path'] = None
        row['dest_mime_type'] = None
    elif normalized["dest_path"] and normalized['mime_type'] != 'inode/directory':
        if (
            str(normalized["dest_path"]).lower() != str(moved_to_dest_path).lower() and
            normalized['moved_to_target'] == 0
        ):
            if moved_to_dest_path.is_file():
                moved_to_dest_path.unlink()

        row['result'] = normalized['result']
        row['moved_to_target'] = normalized['moved_to_target']
        row["dest_path"] = relpath(normalized["dest_path"], start=dest_dir)
        row["dest_mime_type"] = normalized['mime_type']
    elif normalized['mime_type'] == 'inode/directory' and os.path.isdir(dir):
        # if file has been extracted to directory
        if moved_to_dest_path.is_file():
            moved_to_dest_path.unlink()
        row['result'] = Result.SUCCESSFUL
        row["dest_path"] = relpath(normalized["dest_path"], start=source_dir)
        row['dest_mime_type'] = 'inode/directory'

        unpacked_count = sum([len(files) for r, d, files in os.walk(dir)])
        console.print(f'Unpacked {unpacked_count} files', style="bold cyan", end=' ')

        count_before, count_now, total = \
            convert_folder(source_dir, dest_dir,
                           debug, orig_ext, file_storage,
                           row['dest_path'], True)

        file_count += total
    else:
        row['result'] = normalized['result']
        console.print('  ' + row["result"], style="bold red")
        try:
            shutil.copyfile(Path(source_dir, row["source_path"]),
                            moved_to_dest_path)
        except Exception as e:
            print(e)
        if moved_to_dest_path.is_file():
            row["dest_path"] = source_file.path
            row["moved_to_target"] = 1
            row["dest_mime_type"] = source_file.mime_type

    # Without sleep, we sometimes get Operational error:
    # unable to open database file
    # Don't know why
    time.sleep(0.02)
    file_storage.update_row(row["source_path"], list(row.values()))

    return file_count


def write_id_file_to_storage(tsv_source_path: str, source_dir: str,
                             file_storage: ConvertStorage, unpacked_path: str) -> int:
    ext = os.path.splitext(tsv_source_path)[1]

    table = etl.fromtext(tsv_source_path, header=['filename'], strip="\n")
    table = etl.rename(
        table,
        {
            "filename": "source_path",
            "filesize": "source_file_size",
            "mime": "source_mime_type",
            "Content_Type": "source_mime_type",
            "Version": "version",
        },
        strict=False,
    )
    table = etl.select(table, lambda rec: rec.source_path != "")
    table = add_fields(table, "source_mime_type", "version", "dest_path", "result",
                       "dest_mime_type", "puid")
    # Remove Siegfried generated columns
    table = remove_fields(table, "namespace", "basis", "warning")

    table = etl.update(table, 'result', "new")

    # Treat csv (detected from extension only) as plain text:
    table = etl.convert(table, "source_mime_type", lambda v,
                        _row: "text/plain" if _row.id == "x-fmt/18" else v,
                        pass_row=True)

    # Update for missing mime types where id is known:
    table = etl.convert(table, "source_mime_type", lambda v,
                        _row: "application/xml" if _row.id == "fmt/979" else v,
                        pass_row=True)

    if unpacked_path:
        table = etl.convert(table, 'source_path',
                            lambda v: os.path.join(unpacked_path, v))

    file_storage.append_rows(table)
    row_count = etl.nrows(table)
    remove_file(tsv_source_path)
    return row_count


def get_conversion_result(before: int, to_convert: int,
                          total: int) -> tuple[str, str]:
    if before == total:
        return "All files converted previously.", "bold cyan"
    if total - before == to_convert:
        return "All files converted successfully.", "bold green"
    else:
        return ("Not all files were converted. See the db table for details.",
                "bold cyan")


if __name__ == "__main__":
    typer.run(convert)
