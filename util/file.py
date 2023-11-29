from __future__ import annotations
import os
import shutil
import json
import subprocess
import mimetypes
from pathlib import Path
from typing import Optional, Any, List, Callable, Type, Union, Tuple, Dict
from shlex import quote

import magic

from config import cfg, converters
from storage import ConvertStorage
from util import run_shell_command, delete_file_or_dir, extract_nested_zip


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
        self.version = None if unidentify else  row['version']
        self.size = row['size']
        self.puid = None if unidentify else row['puid']
        self.source_id = row['source_id']
        self.parent = Path(self.path).parent
        self.stem = Path(self.path).stem
        self.ext = Path(self.path).suffix
        self.norm = {
            'moved_to_target': 0
        }

    def convert(self, source_dir: str, dest_dir: str, orig_ext: bool,
                debug: bool, identify_only: bool) -> dict[str, Type[str]]:
        """Convert file to archive format"""

        if self.source_id:
            source_path = os.path.join(dest_dir, self.path)
        else:
            source_path = os.path.join(source_dir, self.path)
        dest_path = os.path.join(dest_dir, self.parent, self.stem)
        temp_path = os.path.join('/tmp/convert', self.parent, self.stem)
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

        if self.mime in ['', 'None', None]:
            self.mime = magic.from_file(source_path, mime=True)

        if identify_only:
            return self.normalized, temp_path

        self.norm['mime'] = self.mime

        if self.mime not in converters:
            self.status = 'skipped'
            return self.norm, temp_path

        converter = converters[self.mime]

        if 'puid' in converter and self.puid in converter['puid']:
            converter.update(converter['puid'][self.puid])
        elif 'source-ext' in converter and self.ext in converter['source-ext']:
            converter.update(converter['source-ext'][self.ext])

        accept = False
        if 'accept' in converter:
            if converter['accept'] == True:
                accept = True
            elif 'version' in converter['accept']:
                accept = self.version in converter['accept']['version']

        if accept:
            self.status = 'accepted'
            self.norm['path'] = os.path.join(dest_dir, self.path)
        elif converter.get('remove', False):
            self.status = 'removed'
        elif self.mime == 'application/encrypted':
            self.status = 'protected'
        else:
            temp_path = self._run_conversion_command(converter, source_path, dest_path,
                                                     temp_path, orig_ext, dest_dir, debug)
        if converter.get('keep-original', False) or (self.source_id == None and accept):
            try:
                shutil.copyfile(Path(source_dir, self.path), Path(dest_dir, self.path))
                self.norm['moved_to_target'] = 1
            except Exception as e:
                print('error', e)

        return self.norm, temp_path

    def _run_conversion_command(
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
            converter:        which converter to use
            source_path:      source file path for the file to be converted
            dest_path:        destination file path for where the converted file
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
            cmd = cmd.replace("<source-parent>", quote(str(Path(source_path).parent)))
            cmd = cmd.replace("<dest-parent>", quote(str(Path(dest_path).parent)))
            cmd = cmd.replace("<temp-parent>", quote(str(Path(temp_path).parent)))

        # Disabled because not in use, and file command doesn't have version
        # with option --mime-type
        # cmd = cmd.replace("<version>", '"' + self.version + '"')
        timeout = converter['timeout'] if 'timeout' in converter else cfg['timeout']

        returncode = 0
        if (not os.path.exists(dest_path) or source_path == dest_path) and cmd:

            returncode, out, err = run_shell_command(cmd, cwd=self.pwconv_path,
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
            self.norm['path'] = None
            self.norm['mime'] = None #TODO Sjekk

            if debug:
                print("\nCommand: " + cmd + f" ({returncode})", end="")
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
            self.norm['source_id'] = self.id
            self.norm['path'] = dest_path
            ext = '.' + dest_path.split('.')[-1]
            if os.path.isdir(dest_path):
                self.norm['mime'] = 'inode/directory'
            elif ext in mimetypes.types_map:
                self.norm['mime'] = mimetypes.types_map[ext]
            else:
                self.norm['mime'] = magic.from_file(dest_path, mime=True)

            if self.norm['mime'] != 'inode/directory':
                cmd = ['sf', '-json', dest_path]
                p = subprocess.Popen(cmd, cwd=dest_dir, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE)
                out, err = p.communicate()
                fileinfo = json.loads(out)
                if len(fileinfo['files']):
                    self.norm['mime'] = fileinfo['files'][0]['matches'][0]['mime']
                    self.norm['format'] = fileinfo['files'][0]['matches'][0]['format']
                    self.norm['version'] = fileinfo['files'][0]['matches'][0]['version']
                    self.norm['size'] = fileinfo['files'][0]['filesize']
                    self.norm['puid'] = fileinfo['files'][0]['matches'][0]['id']
                    # self.norm['mime_ext'] = mimetypes.guess_extension(self.norm['mime'])
                    self.norm['ext'] = Path(self.norm['path']).suffix

            if self.norm['path'] == source_path:
                self.norm['status'] = 'accepted'
                self.size = self.norm['size']
                self.puid = self.norm['puid']
                self.format = self.norm['format']
                self.version = self.norm['version']
                self.status = self.norm['status']
            elif self.norm['mime'] != 'inode/directory':
                self.norm['status'] = 'new'

        return temp_path

