import os
import shutil
from pathlib import Path
from typing import Optional, Any, List, Callable, Union, Tuple, Dict

from storage import ConvertStorage
from util import run_shell_command, delete_file_or_dir, extract_nested_zip, Result


class File:
    """Contains methods for converting files"""

    def __init__(self,
                 row: Dict[str, Any],
                 converters: Any,
                 pwconv_path: Path,
                 debug: bool,
                 file_storage: ConvertStorage,
                 convert_folder: Callable[
                     [str, str, ConvertStorage, Optional[bool]
                      ], Union[Tuple[str, int], Tuple[str, int, bool]]
                 ]):
        self.converters = converters
        self.pwconv_path = pwconv_path
        self.debug = debug
        self.convert_folder = convert_folder
        self.file_storage = file_storage
        self.path = row['source_file_path']
        self.mime_type = row['mime_type']
        self.format = row['format']
        self.version = row['version']
        self.file_size = row['file_size']
        self.id = row['id']
        split_ext = os.path.splitext(self.path)
        # relative path without extension
        self.relative_root = split_ext[0]
        self.ext = split_ext[1][1:]
        self.normalized = {
            'norm_file_path': Optional[str], 'result': Optional[str]}

    def convert(self, source_dir: str, target_dir: str):
        """Convert file to archive format"""

        if self.mime_type == 'n/a':
            self.normalized['result'] = Result.NOT_A_DOCUMENT
            self.normalized['norm_file_path'] = None
            return self.normalized
        elif self.mime_type == 'application/zip':
            self._zip_to_norm(source_dir, target_dir)
            return self.normalized

        source_file_path = os.path.join(source_dir, self.path)
        target_file_path = os.path.join(target_dir, self.path)

        if self.mime_type not in self.converters:
            self.normalized['result'] = Result.NOT_SUPPORTED
            self.normalized['norm_file_path'] = None
            return self.normalized

        converter = self.converters[self.mime_type]
        self._run_conversion_command(
            converter, source_file_path, target_file_path, target_dir)

        return self.normalized

    def _run_conversion_command(self, converter: Any, source_file_path: str, target_file_path: str, target_dir: str):
        """
          Convert function

          Args:
              converter: which converter to use
              source_file_path: source file path for the file to be converted
              target_file_path: target file path for where the converted file should be saved
              target_dir: path directory where the converted result should be saved
          """
        cmd, target_ext = self._get_target_ext_and_cmd(converter)
        if target_ext and self.ext != target_ext:
            target_file_path = os.path.join(
                target_dir, f"{self.path}.{target_ext}")

        cmd = cmd.replace('<source>', '"' + source_file_path + '"')
        cmd = cmd.replace('<target>', '"' + target_file_path + '"')
        cmd = cmd.replace('<mime-type>', '"' + self.mime_type + '"')
        cmd = cmd.replace('<target-ext>', '"' + target_ext + '"')
        cmd = cmd.replace('<version>', '"' + self.version + '"')

        result = run_shell_command(cmd, cwd=self.pwconv_path, shell=True)

        if not os.path.exists(target_file_path):
            self.normalized['result'] = Result.FAILED
            self.normalized['norm_file_path'] = None

            if self.debug:
                print("Command: " + cmd)
                # print(str(result[2]))
        else:
            self.normalized['result'] = Result.SUCCESSFUL
            self.normalized['norm_file_path'] = target_file_path

        return result

    def _get_target_ext_and_cmd(self, converter: Any):
        cmd = converter['command']
        
        target_ext = self.ext
        if 'target-ext' in converter:
            target_extensions = converter['target-ext'].split('|')
            for ext in converter['target-ext'].split('|'):
                if ext == self.ext or ext == target_extensions[-1]:
                    target_ext = ext
                    break
        
        # special case for subtypes. For an example see: sdo in converters.yml
        # TODO: This won't work and need rethink or scrapping
        if 'sub-cmds' in converter.keys():
            for sub in converter['sub-cmds']:
                if sub == 'comment':
                    continue
                
                target_mime = (
                    self.mime_type
                    if "target-mime" not in converter["sub-cmds"][sub]
                    else converter["sub-cmds"][sub]["target-mime"]
                    )
                
                sub_cmd = converter['sub-cmds'][sub]['command']
                if target_mime == self.mime_type:                    
                    cmd = sub_cmd + ' && ' + cmd.replace('<source>', '<target>')
                #else:
                    #cmd = sub_cmd                    

        return cmd, target_ext

    def _zip_to_norm(self, source_dir: str, target_dir: str):
        """Exctract all files, convert them, and zip them again"""

        # TODO: Blir sjekk på om normalisert fil finnes nå riktig
        #       for konvertering av zip-fil når ext kan variere?
        # --> Blir skrevet til tsv som 'converted successfully'
        # --> sjekk hvordan det kan stemme når extension på normalsert varierer

        def copy(norm_dir_path_param: str, norm_base_path_param: str):
            files = os.listdir(norm_dir_path_param)
            file = files[0]
            ext = Path(file).suffix
            src = os.path.join(norm_dir_path_param, file)
            dest = os.path.join(
                Path(norm_base_path_param).parent,
                os.path.basename(norm_base_path_param) + '.zip' + ext
            )
            if os.path.isfile(src):
                shutil.copy(src, dest)

        def zip_dir(norm_dir_path_param: str, norm_base_path_param: str):
            shutil.make_archive(base_name=norm_dir_path_param,
                                format='zip', root_dir='.', base_dir=norm_base_path_param)

        def rm_tmp(rm_paths: List[str]):
            for path in rm_paths:
                delete_file_or_dir(path)

        pathToUse = self.path if self.ext != 'zip' else self.relative_root

        working_dir = os.getcwd()
        norm_base_path = os.path.join(target_dir, pathToUse)
        norm_zip_path = norm_base_path + '_zip'
        norm_dir_path = norm_zip_path + '_norm'
        paths = [norm_dir_path + '.tsv', norm_dir_path, norm_zip_path]

        extract_nested_zip(self.path, norm_zip_path)

        result = self.convert_folder(
            norm_zip_path, norm_dir_path, self.file_storage, True)

        self.normalized['result'] = result
        self.normalized['norm_file_path'] = norm_dir_path
        if 'successfully' in result:
            try:
                zip_dir(norm_base_path, norm_dir_path)
            except Exception as e:
                print(e)
                return False
            finally:
                os.chdir(working_dir)

            rm_tmp(paths)

            return True

        os.chdir(working_dir)
        rm_tmp(paths)
        return False
