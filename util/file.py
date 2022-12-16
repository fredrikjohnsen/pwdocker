from __future__ import annotations
import os
import shutil
from pathlib import Path
from typing import Optional, Any, List, Callable, Type, Union, Tuple, Dict

import magic

from storage import ConvertStorage
from util import run_shell_command, delete_file_or_dir, extract_nested_zip, Result


class File:
    """Contains methods for converting files"""

    def __init__(
        self,
        row: Dict[str, Any],
        converters: Any,
        pwconv_path: Path,
        file_storage: ConvertStorage,
        convert_folder: Callable[[str, str, bool, ConvertStorage, bool], Union[Tuple[str, int], Tuple[str, int, bool]]],
    ):
        self.converters = converters
        self.pwconv_path = pwconv_path
        self.convert_folder = convert_folder
        self.row = row
        self.file_storage = file_storage
        self.path = row["source_file_path"]
        self.mime_type = row["mime_type"]
        self.format = row["format"]
        self.version = row["version"]
        self.file_size = row["file_size"]
        self.id = row["id"]
        split_ext = os.path.splitext(self.path)
        # relative path without extension
        self.relative_root = split_ext[0]
        self.ext = split_ext[1][1:]
        self.normalized = {"norm_file_path": Optional[str], "result": Optional[str], "mime_type": Optional[str]}

    def convert(self, source_dir: str, target_dir: str, orig_ext: bool, debug: bool) -> dict[str, Type[str]]:
        """Convert file to archive format"""

        source_file_path = os.path.join(source_dir, self.path)
        if orig_ext:
            target_file_path = os.path.join(target_dir, self.path)
        else:
            target_file_path = os.path.join(target_dir, self.relative_root)
        target_file_path = os.path.abspath(target_file_path)

        if self.mime_type in ['', 'None', None]:
            self.mime_type = magic.from_file(source_file_path, mime=True)

        self.normalized["mime_type"] = self.mime_type

        if self.mime_type == "n/a":
            self.normalized["result"] = Result.NOT_A_DOCUMENT
            self.normalized["norm_file_path"] = None
            return self.normalized
        #elif self.mime_type == "application/zip":
            #self._zip_to_norm(target_dir, debug)
            #return self.normalized

        if self.mime_type not in self.converters:
            self.normalized["result"] = Result.NOT_SUPPORTED
            self.normalized["norm_file_path"] = None
            return self.normalized

        converter = self.converters[self.mime_type]
        self._run_conversion_command(converter, source_file_path, target_file_path, target_dir, orig_ext, debug)

        return self.normalized

    def _run_conversion_command(
            self,
            converter: Any,
            source_file_path: str,
            target_file_path: str,
            target_dir: str,
            orig_ext: bool,
            debug: bool
    ) -> tuple[int, list, list]:
        """
        Convert function

        Args:
            converter: which converter to use
            source_file_path: source file path for the file to be converted
            target_file_path: target file path for where the converted file should be saved
            target_dir: path directory where the converted result should be saved
        """
        cmd, target_ext = self._get_target_ext_and_cmd(converter)
        if not orig_ext or (target_ext and self.ext != target_ext):
            target_file_path = target_file_path + '.' + target_ext

        cmd = cmd.replace("<source>", '"' + source_file_path + '"')
        cmd = cmd.replace("<target>", '"' + target_file_path + '"')
        cmd = cmd.replace("<mime-type>", '"' + self.mime_type + '"')
        cmd = cmd.replace("<target-ext>", '"' + target_ext + '"')
        # Disabled because not in use, and file command doesn't have version
        # with option --mime-type
        # cmd = cmd.replace("<version>", '"' + self.version + '"')

        returncode = run_shell_command(cmd, cwd=self.pwconv_path, shell=True)

        # if not os.path.exists(target_file_path):
        if returncode:
            self.normalized["result"] = Result.FAILED
            self.normalized["norm_file_path"] = None

            if debug:
                print("\nCommand: " + cmd + f" ({returncode})", end="")
        else:
            self.normalized["result"] = Result.SUCCESSFUL
            self.normalized["norm_file_path"] = target_file_path


    def _get_target_ext_and_cmd(self, converter: Any) -> Tuple:
        """
        Extract the target extension and the conversion command

        Args:
            converter: The converter to use
        Returns:
            A Tuple containing the command and target extension
        """

        cmd = converter["command"]

        target_ext = self.ext
        if "target-ext" in converter:
            target_extensions = converter["target-ext"].split("|")
            for ext in converter["target-ext"].split("|"):
                if ext == self.ext or ext == target_extensions[-1]:
                    target_ext = ext
                    break

        # special case for subtypes. For an example see: sdo in converters.yml
        # TODO: This won't work and need rethink or scrapping
        if "sub-cmds" in converter.keys():
            for sub in converter["sub-cmds"]:
                if sub == "comment":
                    continue

                target_mime = (
                    self.mime_type
                    if "target-mime" not in converter["sub-cmds"][sub]
                    else converter["sub-cmds"][sub]["target-mime"]
                )

                sub_cmd = converter["sub-cmds"][sub]["command"]
                if target_mime == self.mime_type:
                    cmd = sub_cmd + " && " + cmd.replace("<source>", "<target>")
                # else:
                # cmd = sub_cmd

        return cmd, target_ext

    def _zip_to_norm(self, target_dir: str, debug: bool) -> None:
        """
        Extract the zipped files, convert them and zip them again.

        Args:
            target_dir: Directory for the resulting zip
        """

        def zip_dir(norm_dir_path_param: str, norm_base_path_param: str):
            return shutil.make_archive(
                base_name=norm_dir_path_param, format="zip", root_dir=norm_base_path_param, base_dir="."
            )

        def rm_tmp(rm_paths: List[str]):
            for path in rm_paths:
                delete_file_or_dir(path)

        path_to_use = self.path if self.ext != "zip" else self.relative_root

        working_dir = os.getcwd()
        norm_base_path = os.path.join(target_dir, path_to_use)
        norm_zip_path = norm_base_path + "_zip"
        norm_dir_path = norm_zip_path + "_norm"
        paths = [norm_dir_path + ".tsv", norm_dir_path, norm_zip_path]

        extract_nested_zip(self.path, norm_zip_path)

        result = self.convert_folder(norm_zip_path, norm_dir_path, self.debug, self.file_storage, True, True)

        if "successfully" in result:
            try:
                norm_zip = zip_dir(norm_base_path, norm_dir_path)
                self.normalized["result"] = Result.SUCCESSFUL
                self.normalized["norm_file_path"] = norm_zip
            except Exception as e:
                self.normalized["result"] = Result.FAILED
                self.normalized["norm_file_path"] = None
        else:
            self.normalized["result"] = Result.FAILED
            self.normalized["norm_file_path"] = None

        os.chdir(working_dir)
        rm_tmp(paths)
