from __future__ import annotations
import os
import shutil
import json
import subprocess
import mimetypes
from os.path import relpath
from inspect import currentframe, getframeinfo
from pathlib import Path
from typing import Any, Type, Dict
from shlex import quote

import magic

from config import cfg, converters
from storage import ConvertStorage
from util import run_shell_cmd


class File:
    """Contains methods for converting files"""

    def __init__(
        self,
        row: Dict[str, Any],
        pwconv_path: Path,
        file_storage: ConvertStorage,
        unidentify: bool
    ):
        self.pwconv_path = pwconv_path
        self.row = row
        self.file_storage = file_storage
        self.id = row['id']
        self.path = row['path']
        self.mime = None if unidentify else row['mime']
        self.format = None if unidentify else row['format']
        self.version = None if unidentify else row['version']
        self.size = row['size']
        self.puid = None if unidentify else row['puid']
        self.source_id = row['source_id']
        self.parent = Path(self.path).parent
        self.stem = Path(self.path).stem
        self.ext = Path(self.path).suffix

    def convert(self, source_dir: str, dest_dir: str, orig_ext: bool,
                debug: bool, identify_only: bool) -> dict[str, Type[str]]:
        """
        Convert file to archive format

        Returns
        - path to converted file
        - False if conversion fails
        - None if file isn't converted
        """

        if self.source_id:
            source_path = os.path.join(dest_dir, self.path)
        else:
            source_path = os.path.join(source_dir, self.path)
        dest_path = os.path.join(dest_dir, self.parent, self.stem)
        tmp_path = os.path.join('/tmp/convert', self.parent, self.stem)
        dest_path = os.path.abspath(dest_path)

        if self.mime in ['', 'None', None]:
            cmd = ['sf', '-json', source_path]
            p = subprocess.Popen(cmd, cwd=source_dir, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
            out, err = p.communicate()

            if not err:
                fileinfo = json.loads(out)
                self.mime = fileinfo['files'][0]['matches'][0]['mime']
                self.format = fileinfo['files'][0]['matches'][0]['format']
                self.version = fileinfo['files'][0]['matches'][0]['version']
                self.size = fileinfo['files'][0]['filesize']
                self.puid = fileinfo['files'][0]['matches'][0]['id']
                basis = fileinfo['files'][0]['matches'][0]['basis']
                if self.mime == 'text/plain' and 'text match' in basis:
                    self.encoding = basis.split('text match ')[1]
                else:
                    self.encoding = None

        if self.mime in ['', 'None', None]:
            self.mime = magic.from_file(source_path, mime=True)

        if identify_only:
            return None

        if self.mime not in converters:
            self.status = 'skipped'
            return None

        converter = converters[self.mime]

        if 'puid' in converter and self.puid in converter['puid']:
            converter.update(converter['puid'][self.puid])
        elif 'source-ext' in converter and self.ext in converter['source-ext']:
            converter.update(converter['source-ext'][self.ext])

        accept = False
        if 'accept' in converter:
            if converter['accept'] is True:
                accept = True
            elif 'version' in converter['accept']:
                accept = self.version in converter['accept']['version']
            elif 'encoding' in converter['accept']:
                accept = self.encoding in converter['accept']['encoding']

        norm_path = None
        if accept:
            self.status = 'accepted'
        elif converter.get('remove', False):
            self.status = 'removed'
        elif self.mime == 'application/encrypted':
            self.status = 'protected'
        else:
            norm_path = self._run_conv_cmd(converter, source_path, dest_path,
                                           tmp_path, orig_ext, dest_dir, debug)

            # TODO: plasser denne inne i `_run_conversion_command` isteden
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        copy_path = Path(dest_dir, self.path)
        if (
            converter.get('keep-original', False) or
            (self.source_id is None and accept) or
            norm_path is False
        ):
            try:
                shutil.copyfile(Path(source_dir, self.path), copy_path)
            except Exception as e:
                frame = getframeinfo(currentframe())
                filename = frame.filename
                line = frame.lineno
                print(filename + ':' + str(line), e)
        elif norm_path or self.status == 'removed':
            # Remove file previously moved to dest because it could
            # not be converted

            dest_path = Path(dest_dir, norm_path)
            if copy_path.is_file() and dest_path != copy_path:
                copy_path.unlink()

        return norm_path

    def _run_conv_cmd(
            self,
            converter: Any,
            source_path: str,
            dest_path: str,
            temp_path: str,
            orig_ext: bool,
            dest_dir: str,
            debug: bool
    ) -> tuple[int, list, list]:
        """
        Convert function

        Args:
            converter:       which converter to use
            source_path:     source file path for the file to be converted
            dest_path:       destination file path for where the converted file
                             should be saved
        """
        cmd = converter["command"] if 'command' in converter else None

        if 'dest-ext' not in converter:
            dest_ext = self.ext
        else:
            dest_ext = (None if converter['dest-ext'] is None
                        else '.' + converter['dest-ext'].strip('.'))

        dest_path = dest_path + dest_ext
        temp_path = temp_path + dest_ext

        if cmd:
            if '<temp>' in cmd:
                Path(Path(temp_path).parent).mkdir(parents=True, exist_ok=True)

            cmd = cmd.replace("<source>", quote(source_path))
            cmd = cmd.replace("<dest>", quote(dest_path))
            cmd = cmd.replace("<temp>", quote(temp_path))
            cmd = cmd.replace("<mime-type>", self.mime)
            cmd = cmd.replace("<dest-ext>", str(dest_ext))
            cmd = cmd.replace("<source-ext>", Path(source_path).suffix)
            cmd = cmd.replace("<source-parent>",
                              quote(str(Path(source_path).parent)))
            cmd = cmd.replace("<dest-parent>",
                              quote(str(Path(dest_path).parent)))
            cmd = cmd.replace("<temp-parent>",
                              quote(str(Path(temp_path).parent)))

        # Disabled because not in use, and file command doesn't have version
        # with option --mime-type
        # cmd = cmd.replace("<version>", '"' + self.version + '"')
        timeout = (converter['timeout'] if 'timeout' in converter
                   else cfg['timeout'])

        returncode = 0
        if (not os.path.exists(dest_path) or source_path == dest_path) and cmd:

            returncode, out, err = run_shell_cmd(cmd, cwd=self.pwconv_path,
                                                 shell=True, timeout=timeout)

        if cmd and (returncode or not os.path.exists(dest_path)):
            if out != 'timeout':
                print('out', out)
                print('err', err)
            if os.path.exists(dest_path):
                # Remove possibel corrupted file
                os.remove(dest_path)
            if 'file requires a password for access' in out:
                self.status = 'protected'
            elif out == 'timeout':
                self.status = 'timeout'
            else:
                self.status = 'failed'

            if debug:
                print("\nCommand: " + cmd + f" ({returncode})", end="")

            return False
        else:
            if orig_ext:
                new_path = os.path.join(dest_dir, self.path)

                if dest_ext:
                    new_path = new_path + dest_ext
                else:
                    new_path = new_path + self.ext

                if new_path != dest_path:
                    os.rename(dest_path, new_path)
                    dest_path = new_path

            self.status = 'converted'
            norm_path = relpath(dest_path, start=dest_dir)

            return norm_path

