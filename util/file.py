from __future__ import annotations
import os
import shutil
import json
import subprocess
import mimetypes
from pathlib import Path
from typing import Optional, Any, List, Callable, Type, Union, Tuple, Dict

import magic

from config import cfg, converters
from storage import ConvertStorage
from util import (run_shell_command, delete_file_or_dir, extract_nested_zip,
                  Result)


class File:
    """Contains methods for converting files"""

    def __init__(
        self,
        row: Dict[str, Any],
        pwconv_path: Path,
        file_storage: ConvertStorage,
        convert_folder: Callable[[str, str, bool, ConvertStorage, bool],
                                 Union[Tuple[str, int], Tuple[str, int, bool]]]
    ):
        self.pwconv_path = pwconv_path
        self.convert_folder = convert_folder
        self.row = row
        self.file_storage = file_storage
        self.path = row["source_path"]
        self.mime_type = row["source_mime_type"]
        self.format = row["format"]
        self.version = row["version"]
        self.file_size = row["source_file_size"]
        self.puid = row['puid']
        split_ext = os.path.splitext(self.path)
        # relative path without extension
        self.relative_root = split_ext[0]
        self.ext = split_ext[1][1:]
        self.normalized = {
            "dest_path": Optional[str],
            "result": Optional[str],
            "mime_type": Optional[str]
        }

    def convert(self, source_dir: str, dest_dir: str, orig_ext: bool,
                debug: bool) -> dict[str, Type[str]]:
        """Convert file to archive format"""

        source_path = os.path.join(source_dir, self.path)
        if orig_ext:
            dest_path = os.path.join(dest_dir, self.path)
        else:
            dest_path = os.path.join(dest_dir, self.relative_root)
        dest_path = os.path.abspath(dest_path)

        if self.mime_type in ['', 'None', None]:
            cmd = ['sf', '-json', source_path]
            p = subprocess.Popen(cmd, cwd=source_dir, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            out, err = p.communicate()
            fileinfo = json.loads(out)
            self.mime_type = fileinfo['files'][0]['matches'][0]['mime']
            self.format = fileinfo['files'][0]['matches'][0]['format']
            self.version = fileinfo['files'][0]['matches'][0]['version']
            self.file_size = fileinfo['files'][0]['filesize']
            self.puid = fileinfo['files'][0]['matches'][0]['id']

        if self.mime_type in ['', 'None', None]:
            self.mime_type = magic.from_file(source_path, mime=True)

        self.normalized["mime_type"] = self.mime_type

        # elif self.mime_type == "application/zip":
        #    self._zip_to_norm(dest_dir, debug)
        #    return self.normalized

        if self.mime_type not in converters:
            self.normalized["result"] = Result.NOT_SUPPORTED
            self.normalized["dest_path"] = None
            return self.normalized

        converter = converters[self.mime_type]
        self._run_conversion_command(converter, source_path, dest_path,
                                     dest_dir, orig_ext, debug)

        return self.normalized

    def _run_conversion_command(
            self,
            converter: Any,
            source_path: str,
            dest_path: str,
            dest_dir: str,
            orig_ext: bool,
            debug: bool
    ) -> tuple[int, list, list]:
        """
        Convert function

        Args:
            converter:        which converter to use
            source_path:      source file path for the file to be converted
            dest_path: target file path for where the converted file
                              should be saved
            dest_dir:         path directory where the converted result
                              should be saved
        """
        cmd, target_ext = self._get_target_ext_and_cmd(converter)
        if not orig_ext or (target_ext and self.ext != target_ext):
            dest_path = dest_path + '.' + target_ext

        cmd = cmd.replace("<source>", '"' + source_path + '"')
        cmd = cmd.replace("<target>", '"' + dest_path + '"')
        cmd = cmd.replace("<mime-type>", '"' + self.mime_type + '"')
        cmd = cmd.replace("<target-ext>", '"' + target_ext + '"')
        # Disabled because not in use, and file command doesn't have version
        # with option --mime-type
        # cmd = cmd.replace("<version>", '"' + self.version + '"')

        returncode = run_shell_command(cmd, cwd=self.pwconv_path, shell=True,
                                       timeout=cfg['timeout'])

        if returncode or not os.path.exists(dest_path):
            self.normalized["result"] = Result.FAILED
            self.normalized["dest_path"] = None
            self.normalized["mime_type"] = None #TODO Sjekk

            if debug:
                print("\nCommand: " + cmd + f" ({returncode})", end="")
        else:
            self.normalized["result"] = Result.SUCCESSFUL
            self.normalized["dest_path"] = dest_path
            ext = '.' + dest_path.split('.')[-1]
            if ext in mimetypes.types_map:
                self.normalized["mime_type"] = mimetypes.types_map[ext]
            else:
                self.normalized["mime_type"] = magic.from_file(dest_path, mime=True)


    def _get_target_ext_and_cmd(self, converter: Any) -> Tuple:
        """
        Extract the target extension and the conversion command

        Args:
            converter: The converter to use
        Returns:
            A Tuple containing the command and target extension
        """

        cmd = converter["command"]

        if 'target-ext' not in converter:
            target_ext = self.ext
        else:
            target_ext = converter['target-ext']

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
                    cmd = sub_cmd + " && " + cmd.replace("<source>",
                                                         "<target>")
                # else:
                # cmd = sub_cmd

        return cmd, target_ext

    def _zip_to_norm(self, dest_dir: str, debug: bool) -> None:
        """
        Extract the zipped files, convert them and zip them again.

        Args:
            dest_dir: Directory for the resulting zip
        """

        def zip_dir(norm_dir_path_param: str, norm_base_path_param: str):
            return shutil.make_archive(base_name=norm_dir_path_param,
                                       format="zip",
                                       root_dir=norm_base_path_param,
                                       base_dir=".")

        def rm_tmp(rm_paths: List[str]):
            for path in rm_paths:
                delete_file_or_dir(path)

        path_to_use = self.path if self.ext != "zip" else self.relative_root

        working_dir = os.getcwd()
        norm_base_path = os.path.join(dest_dir, path_to_use)
        norm_zip_path = norm_base_path + "_zip"
        norm_dir_path = norm_zip_path + "_norm"
        paths = [norm_dir_path + ".tsv", norm_dir_path, norm_zip_path]

        extract_nested_zip(self.path, norm_zip_path)

        result = self.convert_folder(norm_zip_path, norm_dir_path, self.debug, self.file_storage, True, True)

        if "successfully" in result:
            try:
                norm_zip = zip_dir(norm_base_path, norm_dir_path)
                self.normalized["result"] = Result.SUCCESSFUL
                self.normalized["dest_path"] = norm_zip
            except Exception as e:
                self.normalized["result"] = Result.FAILED
                self.normalized["dest_path"] = None
        else:
            self.normalized["result"] = Result.FAILED
            self.normalized["dest_path"] = None

        os.chdir(working_dir)
        rm_tmp(paths)
