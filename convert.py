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
import sys
import pathlib
import shutil
import time
from os.path import relpath
from pathlib import Path
from typing import Dict
import typer

from rich.console import Console
import petl as etl
from petl.io.db import DbView
from ruamel.yaml import YAML

# Load converters
from storage import ConvertStorage, StorageSqliteImpl
from util import make_filelist, remove_file, File, Result
from util.util import get_property_defaults, str_to_bool

yaml = YAML()
# csv.field_size_limit(sys.maxsize)
console = Console()
pwconv_path = pathlib.Path(__file__).parent.resolve()
os.chdir(pwconv_path)

with open(Path(pwconv_path, "converters.yml"), "r") as yamlfile:
    converters = yaml.load(yamlfile)
with open(Path(pwconv_path, "application.yml"), "r") as properties:
    properties = yaml.load(properties)

# Properties set in the local file will overwrite those in application.yml
if os.path.exists(Path(pwconv_path, 'application.local.yml')):
    with open(Path(pwconv_path, "application.local.yml"), "r") as local_properties:
        local_properties = yaml.load(local_properties)
else:
    local_properties = None


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


def convert(source: str, target: str, orig_ext: bool=True, debug: bool=False) -> None:
    Path(target).mkdir(parents=True, exist_ok=True)
    defaults = get_property_defaults(properties, local_properties)

    first_run = False
    db_path = target + '.db'
    if not os.path.isfile(db_path):
        first_run = True

    with StorageSqliteImpl(db_path) as file_storage:
        result, color = convert_folder(source, target, debug, orig_ext,
                                       file_storage, False, first_run)
        console.print(result, style=color)


def convert_folder(
    source_dir: str,
    target_dir: str,
    debug: bool,
    orig_ext: bool,
    file_storage: ConvertStorage,
    zipped: bool,
    first_run: bool
) -> tuple[str, str]:
    """Convert all files in folder"""

    t0 = time.time()
    filelist_path = os.path.join(target_dir, "filelist.txt")
    is_new_batch = os.path.isfile(filelist_path)
    if first_run or is_new_batch:
        if not is_new_batch:
            make_filelist(source_dir, filelist_path)
        tsv_source_path = filelist_path
        write_id_file_to_storage(tsv_source_path, source_dir, file_storage)

    written_row_count = file_storage.get_row_count()

    if is_new_batch:
        unconv_mime_types = file_storage.get_new_mime_types()
    else:
        unconv_mime_types = file_storage.get_unconv_mime_types()

    files_on_disk_count = sum([len(files) for r, d, files in os.walk(source_dir)])
    if files_on_disk_count == 0:
        return "No files to convert. Exiting.", "bold red"
    if files_on_disk_count != written_row_count:
        console.print(f"Row count: {str(written_row_count)}", style="red")
        console.print(f"File count: {str(files_on_disk_count)}", style="red")
        if input(f"Files listed in {file_storage.path} doesn't match files on disk. Continue? [y/n] ") != 'y':
            return "User terminated", "bold red"

    if not zipped:
        console.print("Converting files..", style="bold cyan")

    if first_run:
        files_to_convert_count = written_row_count
        already_converted_count = 0
        table = file_storage.get_all_rows()
    elif is_new_batch:
        table = file_storage.get_new_rows()
        already_converted_count = 0
        files_to_convert_count = etl.nrows(table)
    else:
        # print the files in this directory that have already been converted
        files_to_convert_count, already_converted_count = print_converted_files(
            written_row_count, file_storage
        )
        if files_to_convert_count == 0:
            return "All files converted previously.", "bold cyan"

        table = file_storage.get_unconverted_rows()

    # run conversion:
    table.row_count = 0
    for row in etl.dicts(table):
        table.row_count += 1
        convert_file(files_to_convert_count, file_storage, row, source_dir, table, target_dir, zipped, debug, orig_ext)

    print(str(round(time.time() - t0, 2)) + ' sek')

    # check conversion result
    total_converted_count = etl.nrows(file_storage.get_converted_rows())
    msg, color = get_conversion_result(already_converted_count, files_to_convert_count, total_converted_count)

    return msg, color


def convert_file(
    file_count: int,
    file_storage: ConvertStorage,
    row: Dict[str, any],
    source_dir: str,
    table: DbView,
    target_dir: str,
    zipped: bool,
    debug: bool,
    orig_ext: bool,
) -> None:
    if row['mime_type']:
        # TODO: Why is this necessary?
        row["mime_type"] = row["mime_type"].split(";")[0]
    if not zipped:
        print(end='\x1b[2K') # clear line
        print(f"\r({str(table.row_count)}/{str(file_count)}): {row['source_path']}", end=" ", flush=True)

    source_file = File(row, converters, pwconv_path, file_storage, convert_folder)
    normalized = source_file.convert(source_dir, target_dir, orig_ext, debug)
    row['result'] = normalized['result']
    row['mime_type'] = source_file.mime_type
    row['format'] = source_file.format
    row['file_size'] = source_file.file_size
    row['version'] = source_file.version
    row['puid'] = source_file.puid
    moved_to_target_path = Path(target_dir, row['source_path'])

    if normalized["norm_path"]:
        if str(normalized["norm_path"]) != str(moved_to_target_path):
            if moved_to_target_path.is_file():
                moved_to_target_path.unlink()
            
        row["moved_to_target"] = 0
        row["norm_path"] = relpath(normalized["norm_path"], start=target_dir)
    else:          
        console.print('  ' + row["result"], style="bold red")
        Path(moved_to_target_path.parent).mkdir(parents=True, exist_ok=True)
        try:
            shutil.copyfile(Path(source_dir, row["source_path"]), moved_to_target_path)
        except Exception as e:
            print(e)
        if moved_to_target_path.is_file():
            row["moved_to_target"] = 1
            

    file_storage.update_row(row["source_path"], list(row.values()))


def write_id_file_to_storage(tsv_source_path: str, source_dir: str, file_storage: ConvertStorage) -> int:
    ext = os.path.splitext(tsv_source_path)[1]
    if ext == '.tsv':
        table = etl.fromtsv(tsv_source_path)
    else:
        table = etl.fromtext(tsv_source_path, header=['filename'])
    table = etl.rename(
        table,
        {
            "filename": "source_path",
            "filesize": "file_size",
            "mime": "mime_type",
            "Content_Type": "mime_type",
            "Version": "version",
        },
        strict=False,
    )
    table = etl.select(table, lambda rec: rec.source_path != "")
    # Remove listing of files in zip
    table = etl.select(table, lambda rec: "#" not in rec.source_path)
    table = add_fields(table, "mime_type", "version", "norm_path", "result", "puid")
    # Remove Siegfried generated columns
    table = remove_fields(table, "namespace", "basis", "warning")
    # TODO: Ikke fullgod sjekk på embedded dokument i linje over da # faktisk kan forekomme i filnavn

    # Treat csv (detected from extension only) as plain text:
    table = etl.convert(table, "mime_type", lambda v, _row: "text/plain" if _row.id == "x-fmt/18" else v, pass_row=True)

    # Update for missing mime types where id is known:
    table = etl.convert(
        table, "mime_type", lambda v, _row: "application/xml" if _row.id == "fmt/979" else v, pass_row=True
    )

    file_storage.append_rows(table)
    row_count = etl.nrows(table)
    remove_file(tsv_source_path)
    return row_count


def print_converted_files(total_row_count: int, file_storage: ConvertStorage) -> tuple[int, int]:
    converted_files = file_storage.get_converted_rows()
    already_converted = etl.nrows(converted_files)

    before = total_row_count
    total_row_count -= already_converted
    if already_converted > 0:
        console.print(
            f"({already_converted}/{before}) files have already been converted", style="bold cyan"
        )

    return total_row_count, already_converted


def get_conversion_result(before: int, to_convert: int, total: int) -> tuple[str, str]:
    if total - before == to_convert:
        return "All files converted successfully.", "bold green"
    else:
        return "Not all files were converted. See the db table for details.", "bold cyan"


if __name__ == "__main__":
    typer.run(convert)
