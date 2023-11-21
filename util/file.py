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
            'dest_path': Optional[str],
            'result': Optional[str],
            'mime_type': Optional[str],
            'moved_to_target': 0
        }

    def convert(self, source_dir: str, dest_dir: str, orig_ext: bool,
                debug: bool) -> dict[str, Type[str]]:
        """Convert file to archive format"""

        source_path = os.path.join(source_dir, self.path)

        if orig_ext:
            dest_path = os.path.join(dest_dir, self.path)
            temp_path = os.path.join('/tmp/convert', self.path)
        else:
            dest_path = os.path.join(dest_dir, self.relative_root)
            temp_path = os.path.join('/tmp/convert', self.relative_root)
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

        if self.mime_type not in converters:
            self.normalized["result"] = Result.NOT_SUPPORTED
            self.normalized["dest_path"] = None
            return self.normalized, temp_path

        converter = converters[self.mime_type]
        temp_path = self._run_conversion_command(converter, source_path, dest_path,
                                                 temp_path, orig_ext, debug)

        if converter.get('keep-original', False):
            try:
                shutil.copyfile(Path(source_dir, self.path), Path(dest_dir, self.path))
                self.normalized['moved_to_target'] = 1
            except Exception as e:
                print(e)

        return self.normalized, temp_path

    def _run_conversion_command(
            self,
            converter: Any,
            source_path: str,
            dest_path: str,
            temp_path: str,
            orig_ext: bool,
            debug: bool
    ) -> tuple[int, list, list]:
        """
        Convert function

        Args:
            converter:        which converter to use
            source_path:      source file path for the file to be converted
            dest_path:        destination file path for where the converted file
                              should be saved
        """
        cmd, dest_ext = self._get_dest_ext_and_cmd(converter)
        if dest_ext and (self.ext != dest_ext or not orig_ext):
            dest_path = dest_path + '.' + dest_ext
            temp_path = temp_path + '.' + dest_ext

        if '<temp>' in cmd:
            Path(Path(temp_path).parent).mkdir(parents=True, exist_ok=True)
        if '<unpack-path>' in cmd:
            Path(Path(dest_path)).mkdir(parents=True, exist_ok=True)

        cmd = cmd.replace("<source>", '"' + source_path + '"')
        cmd = cmd.replace("<dest>", '"' + dest_path + '"')
        cmd = cmd.replace("<temp>", '"' + temp_path + '"')
        cmd = cmd.replace("<mime-type>", '"' + self.mime_type + '"')
        cmd = cmd.replace("<dest-ext>", '"' + str(dest_ext) + '"')
        unpack_path = os.path.splitext(source_path)[0]
        cmd = cmd.replace("<unpack-path>", '"' + unpack_path + '"')
        # Disabled because not in use, and file command doesn't have version
        # with option --mime-type
        # cmd = cmd.replace("<version>", '"' + self.version + '"')
        timeout = converter['timeout'] if 'timeout' in converter else cfg['timeout']

        returncode = run_shell_command(cmd, cwd=self.pwconv_path, shell=True,
                                       timeout=timeout)

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
            elif os.path.isdir(dest_path):
                self.normalized["mime_type"] = 'inode/directory'
            else:
                self.normalized["mime_type"] = magic.from_file(dest_path, mime=True)

        return temp_path


    def _get_dest_ext_and_cmd(self, converter: Any) -> Tuple:
        """
        Extract the destination extension and the conversion command

        Args:
            converter: The converter to use
        Returns:
            A Tuple containing the command and destination extension
        """

        cmd = converter["command"]

        if 'dest-ext' not in converter:
            dest_ext = self.ext
        else:
            dest_ext = converter['dest-ext']

        # special case for subtypes. For an example see: sdo in converters.yml
        # TODO: This won't work and need rethink or scrapping
        if "sub-cmds" in converter.keys():
            for sub in converter["sub-cmds"]:
                if sub == "comment":
                    continue

                dest_mime = (
                    self.mime_type
                    if "dest-mime" not in converter["sub-cmds"][sub]
                    else converter["sub-cmds"][sub]["dest-mime"]
                )

                sub_cmd = converter["sub-cmds"][sub]["command"]
                if dest_mime == self.mime_type:
                    cmd = sub_cmd + " && " + cmd.replace("<source>",
                                                         "<dest>")
                # else:
                # cmd = sub_cmd

        return cmd, dest_ext

