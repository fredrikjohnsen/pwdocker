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
from argparse import ArgumentParser, Namespace

from rich.console import Console
import petl as etl
from petl.io.db import DbView
from ruamel.yaml import YAML

# Load converters
from storage import ConvertStorage, StorageSqliteImpl
from util import run_siegfried, run_file_command, mime_from_ext, remove_file, File, Result
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
with open(Path(pwconv_path, "application.local.yml"), "r") as local_properties:
    local_properties = yaml.load(local_properties)


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


def convert_folder_entrypoint(args: Namespace) -> None:
    Path(args.target).mkdir(parents=True, exist_ok=True)

    first_run = False
    if not os.path.isfile(args.db_path):
        first_run = True

    with StorageSqliteImpl(args.db_path, args.resume) as file_storage:
        result, color = convert_folder(args.source, args.target, args.debug,
                                       args.keep_ext, args.identifier, args.confirm,
                                       file_storage,
                                       False, first_run)
        console.print(result, style=color)


def convert_folder(
    source_dir: str,
    target_dir: str,
    debug: bool,
    keep_ext: bool,
    identifier: str,
    confirm: bool,
    file_storage: ConvertStorage,
    zipped: bool,
    first_run: bool
) -> tuple[str, str]:
    """Convert all files in folder"""

    t0 = time.time()
    filelist_path = os.path.join(args.target, "filelist.txt")
    if first_run or os.path.isfile(filelist_path):
        if not zipped:
            console.print("Identifying file types...", style="bold cyan")

        tsv_source_path = target_dir + ".tsv"
        if identifier == 'sf':
            run_siegfried(args.source, target_dir, tsv_source_path, False)
        elif identifier == 'file':
            run_file_command(args.source, target_dir, tsv_source_path, False)
        else:
            mime_from_ext(args.source, target_dir, tsv_source_path, False)
        write_id_file_to_storage(tsv_source_path, source_dir, file_storage)

    written_row_count = file_storage.get_row_count()

    unconv_mime_types = file_storage.get_unconv_mime_types(source_dir)
    missing_mime_types = etl.select(unconv_mime_types, lambda rec: rec.mime_type not in converters)
    if etl.nrows(missing_mime_types):
        print("Following file types haven't got a converter:")
        print(missing_mime_types)
        if not confirm and input("Do you wish to continue [y/n]: ") != 'y':
            return "User terminated", "bold red"

    files_on_disk_count = sum([len(files) for r, d, files in os.walk(source_dir)])
    if files_on_disk_count == 0:
        return "No files to convert. Exiting.", "bold red"
    if files_on_disk_count != written_row_count:
        console.print(f"Row count: {str(written_row_count)}", style="red")
        console.print(f"File count: {str(files_on_disk_count)}", style="red")
        return f"Files listed in {args.db_path} doesn't match files on disk. Exiting.", "bold red"
    if not zipped:
        console.print("Converting files..", style="bold cyan")

    if first_run:
        files_to_convert_count = written_row_count
        already_converted_count = 0
        table = file_storage.get_all_rows(source_dir)
    else:
        # print the files in this directory that have already been converted
        files_to_convert_count, already_converted_count = print_converted_files(
            written_row_count, file_storage, source_dir
        )
        if files_to_convert_count == 0:
            return "All files converted previously.", "bold cyan"

        table = file_storage.get_unconverted_rows(source_dir)

    # run conversion:
    convert_files(files_to_convert_count, source_dir, table, target_dir, file_storage, zipped, debug, keep_ext)

    print(str(round(time.time() - t0, 2)) + ' sek')

    # check conversion result
    total_converted_count = etl.nrows(file_storage.get_converted_rows(source_dir))
    msg, color = get_conversion_result(already_converted_count, files_to_convert_count, total_converted_count)

    return msg, color


def convert_files(
    file_count: int,
    source_dir: str,
    table: DbView,
    target_dir: str,
    file_storage: ConvertStorage,
    zipped: bool,
    debug: bool,
    keep_ext: bool
) -> None:
    table.row_count = 0
    for row in etl.dicts(table):
        source_file = Path(os.path.basename(row["source_file_path"]))
        if source_file.is_symlink() or source_file.name == "Thumbs.db":
            remove_file(row["source_file_path"])
            row["result"] = Result.AUTOMATICALLY_DELETED
            file_storage.update_row(row["source_file_path"], row["source_directory"], list(row.values()))
            file_count -= 1
            continue

        table.row_count += 1
        convert_file(file_count, file_storage, row, source_dir, table, target_dir, zipped, debug, keep_ext)


def convert_file(
    file_count: int,
    file_storage: ConvertStorage,
    row: Dict[str, any],
    source_dir: str,
    table: DbView,
    target_dir: str,
    zipped: bool,
    debug: bool,
    keep_ext: bool,
) -> None:        
    row["mime_type"] = row["mime_type"].split(";")[0]
    if not row["mime_type"]:
        # Siegfried sets mime type only to xml files with xml declaration
        if os.path.splitext(row["source_file_path"])[1].lower() == ".xml":
            row["mime_type"] = "application/xml"
    if not zipped:
        print(end='\x1b[2K') # clear line
        print(f"\r({str(table.row_count)}/{str(file_count)}): {row['source_file_path']} ({row['mime_type']})", end=" ", flush=True)

    source_file = File(row, converters, pwconv_path, debug, file_storage, convert_folder)
    normalized = source_file.convert(source_dir, target_dir, keep_ext)
    row["result"] = normalized["result"]
    row["mime_type"] = normalized["mime_type"]
    moved_to_target_path = Path(target_dir, row["source_file_path"])

    if normalized["norm_file_path"]:        
        if str(normalized["norm_file_path"]) != str(moved_to_target_path):
            if moved_to_target_path.is_file():
                moved_to_target_path.unlink()
            
        row["moved_to_target"] = 0
        row["norm_file_path"] = relpath(normalized["norm_file_path"], start=target_dir)
    else:          
        console.print('  ' + row["result"], style="bold red")
        Path(moved_to_target_path.parent).mkdir(parents=True, exist_ok=True)
        try:
            shutil.copyfile(Path(source_dir, row["source_file_path"]), moved_to_target_path)
        except Exception as e:
            print(e)
        if moved_to_target_path.is_file():
            row["moved_to_target"] = 1
            

    file_storage.update_row(row["source_file_path"], row["source_directory"], list(row.values()))


def write_id_file_to_storage(tsv_source_path: str, source_dir: str, file_storage: ConvertStorage) -> int:
    table = etl.fromtsv(tsv_source_path)
    table = etl.rename(
        table,
        {
            "filename": "source_file_path",
            "tika_batch_fs_relative_path": "source_file_path",
            "filesize": "file_size",
            "mime": "mime_type",
            "Content_Type": "mime_type",
            "Version": "version",
        },
        strict=False,
    )
    table = etl.select(table, lambda rec: rec.source_file_path != "")
    # Remove listing of files in zip
    table = etl.select(table, lambda rec: "#" not in rec.source_file_path)
    table = add_fields(table, "version", "norm_file_path", "result", "id")
    table = etl.addfield(table, "source_directory", source_dir)
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


def print_converted_files(total_row_count: int, file_storage: ConvertStorage, source_dir: str) -> tuple[int, int]:
    converted_files = file_storage.get_converted_rows(source_dir)
    already_converted = etl.nrows(converted_files)

    before = total_row_count
    total_row_count -= already_converted
    if already_converted > 0:
        console.print(
            f"({already_converted}/{before}) files have already been converted  in {source_dir}", style="bold cyan"
        )

    return total_row_count, already_converted


def get_conversion_result(before: int, to_convert: int, total: int) -> tuple[str, str]:
    if total - before == to_convert:
        return "All files converted successfully.", "bold green"
    else:
        return "Not all files were converted. See the db table for details.", "bold cyan"


def create_args_parser(parser: ArgumentParser):
    defaults = get_property_defaults(properties, local_properties)
    parser.add_argument(
        "-s",
        "--source",
        help="Absolute path to the source directory. Default: " + defaults['directories']['source'],
        default=defaults["directories"]["source"]
    )
    parser.add_argument(
        "-t",
        "--target",
        help="Absolute path to the target directory. Default: " + defaults['directories']['target'],
        default=defaults["directories"]["target"]
    )
    parser.add_argument(
        "-dp",
        "--db-path",
        help="Absolute path to the database file. Default: " + defaults['database']['path'],
        default=defaults["database"]["path"]
    )
    parser.add_argument(
        "-r",
        "--resume",
        help="""Resume a previous conversion.
        False to convert all files in the folder.
        Default: """ + str(defaults['database']['continue-conversion']),
        default=defaults["database"]["continue-conversion"],
        type=lambda x: str_to_bool(x),
        choices=(True, False),
    )
    parser.add_argument(
        "-d",
        "--debug",
        help="Print commands. Default: " + str(defaults['options']['debug']),
        default=defaults["options"]["debug"],
        type=lambda x: str_to_bool(x),
        choices=(True, False),
    )
    parser.add_argument(
        "-ke",
        "--keep-ext",
        help="Add original extension to file name. Default: " + str(defaults['options']['keep-ext']),
        default=defaults["options"]["keep-ext"],
        type=lambda x: str_to_bool(x),
        choices=(True, False)
    )
    parser.add_argument(
        "-i",
        "--identifier",
        help="File type identifier. Default: " + defaults['options']['file-type-identifier'],
        default=defaults["options"]["file-type-identifier"],
        choices=("sf", "file")
    )
    parser.add_argument(
        "-c",
        "--confirm",
        help="Confirm automatically to continue if missing conversion for specific file types",
        default=False,
        type=lambda x: str_to_bool(x),
        choices=(True, False)
    )


parser = ArgumentParser("convert.py")
create_args_parser(parser)

if __name__ == "__main__":
    args = parser.parse_args()
    convert_folder_entrypoint(args)
