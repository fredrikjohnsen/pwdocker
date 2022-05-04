# Copyright(C) 2021 Morten Eek

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

# from PIL import Image
import typer
import os
import subprocess
import shutil
import signal
import zipfile
import re
import pathlib
# import img2pdf
import petl as etl
import base64
import csv
import glob
from os.path import relpath
from pathlib import Path
import cchardet as chardet
# from pathlib import Path
# from functools import reduce
# import wand
# from wand.image import Image, Color
# from wand.exceptions import BlobError
if os.name == "posix":
    # import ocrmypdf
    from pdfy import Pdfy


class File:

    def __init__(self, path, mime_type='text/plain', version=None):
        self.path = path
        self.mime_type = mime_type
        self.version = version

    def append_tsv_row(self, row):
        with open(self.path, 'a') as tsv_file:
            writer = csv.writer(
                tsv_file,
                delimiter='\t',
                quoting=csv.QUOTE_NONE,
                quotechar='',
                lineterminator='\n',
                escapechar='')
            writer.writerow(row)

    def append_txt(self, msg):
        with open(self.path, 'a') as txt_file:
            txt_file.write(msg + '\n')

    def convert(self, target_dir, norm_file_path=None):
        source_file_name = os.path.basename(self.path)
        split_ext = os.path.splitext(source_file_name)
        base_file_name = split_ext[0]
        ext = split_ext[1]
        tmp_dir = os.path.join(target_dir, 'pw_tmp')
        tmp_file_path = tmp_dir + '/' + base_file_name + 'tmp'
        if self.mime_type not in mime_to_norm:
            mime_to_norm[self.mime_type] = (None, None)
        function = mime_to_norm[self.mime_type][0]

        # Ensure unique file names in dir hierarchy:
        norm_ext = mime_to_norm[self.mime_type][1]
        if not norm_ext:
            norm_ext = 'none'

        if norm_file_path is None:
            norm_file_path = target_dir + '/' + base_file_name + ext
            if (norm_ext and '.'+norm_ext != ext) :
                norm_file_path = norm_file_path + '.' + norm_ext

        # TODO: Endre så returneres file paths som starter med prosjektmappe? Alltid, eller bare når genereres arkivpakke?
        normalized = {'result': None, 'norm_file_path': norm_file_path, 'error': None, 'original_file_copy': None}

        if not check_for_files(norm_file_path + '*'):
            if self.mime_type == 'n/a':
                normalized['msg'] = 'Not a document'
                normalized['norm_file_path'] = None
            elif function:
                pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True)

                function_args = {'source_file_path': self.path,
                                'tmp_file_path': tmp_file_path,
                                'norm_file_path': norm_file_path,
                                'tmp_dir': tmp_dir,
                                'mime_type': self.mime_type,
                                'version': self.version,
                                }

                converter = Converter(self.mime_type)
                ok = converter.run(function_args)

                if not ok:
                    error_files = target_dir + '/error_documents/'
                    pathlib.Path(error_files).mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(self.path, error_files + os.path.basename(self.path))
                    normalized['original_file_copy'] = error_files + os.path.basename(self.path)  # TODO: Fjern fil hvis konvertering lykkes når kjørt på nytt
                    normalized['msg'] = 'Conversion failed'
                    normalized['norm_file_path'] = None
                else:
                    normalized['msg'] = 'Converted successfully'
            else:
                normalized['msg'] = 'Conversion not supported'
                normalized['norm_file_path'] = None
        else:
            normalized['msg'] = 'Manually converted'

        if os.path.isfile(tmp_file_path):
            os.remove(tmp_file_path)

        return normalized

class Converter:

    def __init__(self, mime):
        self.mime = mime

    def run(self, args):
        result = False
        if self.mime == 'text/plain':
            result = self.x2utf8(args)
        elif self.mime == 'message/rfc822':
            result = self.eml2pdf(args)
        elif self.mime == 'multipart/related':
            result = self.mhtml2pdf(args)
        elif self.mime == 'application/zip':
            result = self.zip_to_norm(args)
        elif self.mime in ('image/gif', 'image/tiff'):
            result = self.image2norm(args)
        elif self.mime in (
            'application/msword', 'application/rtf', 'application/vnd.ms-excel',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            'application/vnd.wordperfect'
        ):
            result = self.docbuilder2x(args)
        elif self.mime == 'application/xhtml+xml':
            result = self.wkhtml2pdf(args)
        elif self.mime == 'application/pdf':
            result = self.pdf2pdfa(args)
        elif self.mime == 'text/html':
            result = self.html2pdf(args)
        else:
            shutil.copyfile(args['source_file_path'], args['norm_file_path'])


        return result

    def x2utf8(self, args):
        # TODO: Sjekk om beholder extension alltid (ikke endre csv, xml mm)
        # TODO: Test å bruke denne heller enn/i tillegg til replace under: https://ftfy.readthedocs.io/en/latest/

        repls = (
            ('‘', 'æ'),
            ('›', 'ø'),
            ('†', 'å'),
            ('&#248;', 'ø'),
            ('&#229;', 'å'),
            ('&#230;', 'æ'),
            ('&#197;', 'Å'),
            ('&#216;', 'Ø'),
            ('&#198;', 'Æ'),
            ('=C2=A0', ' '),
            ('=C3=A6', 'æ'),
            ('=C3=B8', 'ø'),
            ('=C3=A5', 'å'),
        )

        with open(args['norm_file_path'], "wb") as file:
            with open(args['source_file_path'], 'rb') as file_r:
                content = file_r.read()
                if content is None:
                    return False

                char_enc = chardet.detect(content)['encoding']

                try:
                    data = content.decode(char_enc)
                except Exception:
                    return False

                for k, v in repls:
                    data = re.sub(k, v, data, flags=re.MULTILINE)
            file.write(data.encode('utf8'))

            if os.path.exists(args['norm_file_path']):
                return True

        return False


    def eml2pdf(self, args):
        ok = False
        args['tmp_file_path'] = args['tmp_file_path'] + '.pdf'
        command = ['eml_to_pdf', args['source_file_path'], args['tmp_file_path']]
        run_shell_command(command)

        if os.path.exists(args['tmp_file_path']):
            ok = self.pdf2pdfa(args)

            if os.path.isfile(args['tmp_file_path']):
                os.remove(args['tmp_file_path'])

        return ok


    def mhtml2pdf(self, args):
        ok = False
        args['tmp_file_path'] = args['tmp_file_path'] + '.pdf'
        java_path = os.environ['pwcode_java_path']  # Get Java home path
        converter_jar = os.path.expanduser("~") + '/bin/emailconverter/emailconverter.jar'

        command = [java_path, '-jar', converter_jar, '-e', args['source_file_path'], '-o', args['tmp_file_path']]
        result = run_shell_command(command)
        # print(result)

        if os.path.exists(args['tmp_file_path']):
            ok = self.pdf2pdfa(args)

            if os.path.isfile(args['tmp_file_path']):
                os.remove(args['tmp_file_path'])

        return ok


    def zip_to_norm(self, args):
        # TODO: Blir sjekk på om normalisert fil finnes nå riktig for konvertering av zip-fil når ext kan variere?
        # --> Blir skrevet til tsv som 'converted successfully' -> sjekk hvordan det kan stemme når extension på normalsert varierer

        def copy(norm_dir_path, norm_base_path):
            files = os.listdir(norm_dir_path)
            file = files[0]
            ext = Path(file).suffix
            src = os.path.join(norm_dir_path, file)
            dest = os.path.join(Path(norm_base_path).parent, os.path.basename(norm_base_path) + '.zip' + ext)
            if os.path.isfile(src):
                shutil.copy(src, dest)

        def zip_dir(norm_dir_path, norm_base_path):
            shutil.make_archive(norm_base_path, 'zip', norm_dir_path)

        def rm_tmp(paths):
            for path in paths:
                delete_file_or_dir(path)

        norm_base_path = os.path.splitext(args['norm_file_path'])[0]
        norm_zip_path = norm_base_path + '_zip'
        norm_dir_path = norm_zip_path + '_norm'
        paths = [norm_dir_path + '.tsv', norm_dir_path, norm_zip_path]

        extract_nested_zip(args['source_file_path'], norm_zip_path)

        msg, file_count, errors, originals = convert_folder(norm_zip_path, norm_dir_path, args['tmp_dir'], zipped=True)

        if 'succcessfully' in msg:
            func = copy

            if file_count > 1:
                func = zip_dir

            try:
                func(norm_dir_path, norm_base_path)
            except Exception as e:
                print(e)
                return False

            rm_tmp(paths)
            if originals:
                return 'originals'
            else:
                return True

        rm_tmp(paths)
        return False


    def image2norm(self, args):
        ok = False
        args['tmp_file_path'] = args['tmp_file_path'] + '.pdf'
        command = ['convert', args['source_file_path'], args['tmp_file_path']]
        run_shell_command(command)

        if os.path.exists(args['tmp_file_path']):
            ok = self.pdf2pdfa(args)

            # WAIT: Egen funksjon for sletting av tmp-filer som kalles fra alle def? Er nå under her for å håndtere endret tmp navn + i overordnet convert funkson
            if os.path.isfile(args['tmp_file_path']):
                os.remove(args['tmp_file_path'])

        return ok


    def docbuilder2x(self, args):
        ok = False
        docbuilder_file = os.path.join(args['tmp_dir'], 'x2x.docbuilder')

        docbuilder = None
        # WAIT: Tremger ikke if/else under hvis ikke skal ha spesifikk kode pr format
        if args['mime_type'] in (
                'application/vnd.ms-excel',
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        ):
            docbuilder = [
                'builder.OpenFile("' + args['source_file_path'] + '", "")',
                'builder.SaveFile("pdf", "' + args['tmp_file_path'] + '")',
                'builder.CloseFile();',
            ]
        else:
            docbuilder = [
                'builder.OpenFile("' + args['source_file_path'] + '", "")',
                'builder.SaveFile("pdf", "' + args['tmp_file_path'] + '")',
                'builder.CloseFile();',
            ]

        with open(docbuilder_file, "w+") as file:
            file.write("\n".join(docbuilder))

        command = ['documentbuilder', docbuilder_file]
        run_shell_command(command)

        if os.path.exists(args['tmp_file_path']):
            ok = self.pdf2pdfa(args)

        return ok


    def wkhtml2pdf(self, args):
        # WAIT: Trengs sjekk om utf-8 og evt. konvertering først her?
        ok = False
        command = ['wkhtmltopdf', '-O', 'Landscape', args['source_file_path'], args['tmp_file_path']]
        run_shell_command(command)

        if os.path.exists(args['tmp_file_path']):
            ok = self.pdf2pdfa(args)

        return ok


    def abi2x(self, args):
        ok = False
        command = ['abiword', '--to=pdf', '--import-extension=rtf', '-o', args['tmp_file_path'], args['source_file_path']]
        run_shell_command(command)

        # TODO: Må ha bedre sjekk av generert pdf. Har laget tomme pdf-filer noen ganger
        if os.path.exists(args['tmp_file_path']):
            ok = self.pdf2pdfa(args)

        return ok


    def pdf2pdfa(self, args):
        ok = False

        if args['mime_type'] == 'application/pdf':
            args['tmp_file_path'] = args['source_file_path']

            # WAIT: Legg inn ekstra sjekk her om hva som skal gjøres hvis ocr = True
            if args['version'] in ('1a', '1b', '2a', '2b'):
                shutil.copyfile(args['source_file_path'], args['norm_file_path'])
                if os.path.exists(args['norm_file_path']):
                    ok = True

                return ok

        ocrmypdf.configure_logging(-1)
        # Set tesseract_timeout=0 to only do PDF/A-conversion, and not ocr
        result = ocrmypdf.ocr(args['tmp_file_path'], args['norm_file_path'], tesseract_timeout=0, progress_bar=False, skip_text=True)
        if str(result) == 'ExitCode.ok':
            ok = True

        return ok


    def html2pdf(self, args):
        ok = False
        try:
            p = Pdfy()
            p.html_to_pdf(args['source_file_path'], args['tmp_file_path'])
        except Exception as e:
            print(e)

        if os.path.exists(args['tmp_file_path']):
            ok = self.pdf2pdfa(args)

        return ok

    # def libre2x(source_file_path, tmp_file_path, norm_file_path, tmp_dir, mime_type):
        # TODO: Endre så bruker collabora online (er installert på laptop). Se notater om curl kommando i joplin
        # command = ["libreoffice", "--convert-to", "pdf", "--outdir", str(filein.parent), str(filein)]
        # run_shell_command(command)


    def unoconv2x(self, file_path, norm_path, format, file_type):
        ok = False
        command = ['unoconv', '-f', format]

        if file_type in (
                'application/vnd.ms-excel',
                'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        ):
            if format == 'pdf':
                command.extend([
                    '-d', 'spreadsheet', '-P', 'PaperOrientation=landscape',
                    '-eSelectPdfVersion=1'
                ])
            elif format == 'html':
                command.extend(
                    ['-d', 'spreadsheet', '-P', 'PaperOrientation=landscape'])
        elif file_type in ('application/msword', 'application/rtf'):
            command.extend(['-d', 'document', '-eSelectPdfVersion=1'])

        command.extend(['-o', '"' + norm_path + '"', '"' + file_path + '"'])
        run_shell_command(command)

        if os.path.exists(norm_path):
            ok = True

        return ok


def run_siegfried(source_dir, tmp_dir, tsv_path, zipped=False):
    if not zipped:
        print('\nIdentifying file types...')

    csv_path = os.path.join(tmp_dir, 'tmp.csv')
    os.chdir(source_dir)
    subprocess.run(
        'sf -z -csv * > ' + csv_path,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        shell=True,
    )

    with open(csv_path, 'r') as csvin, open(tsv_path, 'w') as tsvout:
        csvin = csv.reader(csvin)
        tsvout = csv.writer(tsvout, delimiter='\t')
        for row in csvin:
            tsvout.writerow(row)

    if os.path.exists(csv_path):
        os.remove(csv_path)


def delete_file_or_dir(path):
    if os.path.isfile(path):
        os.remove(path)

    if os.path.isdir(path):
        shutil.rmtree(path)


def check_for_files(filepath):
    for filepath_object in glob.glob(filepath):
        if os.path.isfile(filepath_object):
            return True

    return False


# mime_type: (function name, new file extension)
mime_to_norm = {
    'application/msword': ('docbuilder2x', 'pdf'),
    'application/pdf': ('pdf2pdfa', 'pdf'),
    # 'application/rtf': ('abi2x', 'pdf'),
    'application/rtf': ('docbuilder2x', 'pdf'),  # TODO: Finn beste test på om har blitt konvertert til pdf
    'application/vnd.ms-excel': ('docbuilder2x', 'pdf'),
    # 'application/vnd.ms-project': ('pdf'), # TODO: Har ikke ferdig kode for denne ennå
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': ('docbuilder2x', 'pdf'),
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ('docbuilder2x', 'pdf'),
    'application/vnd.openxmlformats-officedocument.presentationml.presentation': ('docbuilder2x', 'pdf'),
    'application/vnd.wordperfect': ('docbuilder2x', 'pdf'),  # TODO: Mulig denne må endres til libreoffice
    # 'application/xhtml+xml; charset=UTF-8': ('wkhtmltopdf', 'pdf'),
    'application/xhtml+xml': ('wkhtmltopdf', 'pdf'),
    # 'application/xml': (False, 'file_copy', 'xml'),
    'application/xml': ('x2utf8', 'xml'),
    'application/x-elf': (None, None),  # executable on lin
    'application/x-msdownload': (None, None),  # executable on win
    'application/x-ms-installer': (None, None),  # Installer on win
    'application/x-tika-msoffice': (None, None),
    'n/a': (None, None),
    'application/zip': ('zip_to_norm', 'zip'),
    'image/gif': ('image2norm', 'pdf'),
    # 'image/jpeg': ('image2norm', 'pdf'),
    'image/jpeg': ('file_copy', 'jpg'),
    'image/png': ('file_copy', 'png'),
    'image/tiff': ('image2norm', 'pdf'),
    'text/html': ('html2pdf', 'pdf'),  # TODO: Legg til undervarianter her (var opprinnelig 'startswith)
    'text/plain': ('x2utf8', 'txt'),
    'multipart/related': ('mhtml2pdf', 'pdf'),
    'message/rfc822': ('eml2pdf', 'pdf'),
}


def extract_nested_zip(zipped_file, to_folder):
    with zipfile.ZipFile(zipped_file, 'r') as zfile:
        zfile.extractall(path=to_folder)

    for root, dirs, files in os.walk(to_folder):
        for filename in files:
            if re.search(r'\.zip$', filename):
                fileSpec = os.path.join(root, filename)
                extract_nested_zip(fileSpec, root)


def kill(proc_id):
    os.kill(proc_id, signal.SIGINT)


def run_shell_command(command, cwd=None, timeout=30):
    # ok = False
    os.environ['PYTHONUNBUFFERED'] = "1"
    # cmd = [' '.join(command)]
    stdout = []
    stderr = []
    mix = []  # TODO: Fjern denne mm

    # print(''.join(cmd))
    # sys.stdout.flush()

    proc = subprocess.Popen(
        command,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        kill(proc.pid)

    # while proc.poll() is None:
    #     line = proc.stdout.readline()
    #     if line != "":
    #         stdout.append(line)
    #         mix.append(line)
    #         print(line, end='')

    #     line = proc.stderr.readline()
    #     if line != "":
    #         stderr.append(line)
    #         mix.append(line)
    #         print(line, end='')

    for line in proc.stdout:
        stdout.append(line.rstrip())

    for line in proc.stderr:
        stderr.append(line.rstrip())

    # print(stderr)
    return proc.returncode, stdout, stderr, mix


# TODO: Hvordan kalle denne med python: tesseract my-image.png nytt filnavn pdf -> må bruke subprocess



def remove_fields(fields, table):
    for field in fields:
        if field in etl.fieldnames(table):
            table = etl.cutout(table, field)
    return table


def add_fields(fields, table):
    for field in fields:
        if field not in etl.fieldnames(table):
            table = etl.addfield(table, field, None)
    return table


def convert_folder(source_dir: str, target_dir: str,
                   zipped: bool=False):
    # WAIT: Legg inn i gui at kan velge om skal ocr-behandles
    txt_target_path = target_dir + '_result.txt'
    tsv_source_path = target_dir + '.tsv'
    tsv_target_path = target_dir + '_processed.tsv'
    tmp_dir = os.path.join(target_dir, 'pw_tmp')
    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)
    converted_now = False
    errors = False
    originals = False

    result_file = File(txt_target_path)

    if os.path.exists(tsv_target_path):
        os.remove(tsv_target_path)

    Path(target_dir).mkdir(parents=True, exist_ok=True)

    # TODO: Viser mime direkte om er pdf/a eller må en sjekke mot ekstra felt i de to under? Forsjekk om Tika og siegfried?

    # TODO: Trengs denne sjekk om tsv her. Gjøres sjekk før kaller denne funskjonen og slik at unødvendig?
    # if not os.path.isfile(tsv_source_path):
    if True:
        run_siegfried(source_dir, tmp_dir, tsv_source_path, zipped)

    # TODO: Legg inn test på at tsv-fil ikke er tom

    table = etl.fromtsv(tsv_source_path)
    table = etl.rename(table,
                       {
                           'filename': 'source_file_path',
                           'tika_batch_fs_relative_path': 'source_file_path',
                           'filesize': 'file_size',
                           'mime': 'mime_type',
                           'Content_Type': 'mime_type',
                           'Version': 'version'
                       },
                       strict=False)

    thumbs_table = etl.select(table, lambda rec: Path(rec.source_file_path).name == 'Thumbs.db')
    if etl.nrows(thumbs_table) > 0:
        thumbs_paths = etl.values(thumbs_table, 'source_file_path')
        for path in thumbs_paths:
            if '/' not in path:
                path = os.path.join(source_dir, path)
            if os.path.isfile(path):
                os.remove(path)

        table = etl.select(table, lambda rec: Path(rec.source_file_path).name != 'Thumbs.db')

    table = etl.select(table, lambda rec: rec.source_file_path != '')
    table = etl.select(table, lambda rec: '#' not in rec.source_file_path)
    # WAIT: Ikke fullgod sjekk på embedded dokument i linje over da # faktisk kan forekomme i filnavn
    row_count = etl.nrows(table)

    file_count = sum([len(files) for r, d, files in os.walk(source_dir)])

    if row_count == 0:
        print('No files to convert. Exiting.')
        return 'Error', file_count
    elif file_count != row_count:
        print('Row count: ' + str(row_count))
        print('File count: ' + str(file_count))
        print("Files listed in '" + tsv_source_path + "' doesn't match files on disk. Exiting.")
        return 'Error', file_count
    elif not zipped:
        print('Converting files..')

    # WAIT: Legg inn sjekk på filstørrelse før og etter konvertering

    append_fields = ('version', 'norm_file_path', 'result', 'original_file_copy', 'id')
    table = add_fields(append_fields, table)

    # Remove Siegfried generated columns
    cut_fields = ('namespace', 'basis', 'warning')
    table = remove_fields(cut_fields, table)

    header = etl.header(table)

    tsv_file = File(tsv_target_path)
    tsv_file.append_tsv_row(header)

    # Treat csv (detected from extension only) as plain text:
    table = etl.convert(table, 'mime_type', lambda v, row: 'text/plain' if row.id == 'x-fmt/18' else v, pass_row=True)

    # Update for missing mime types where id is known:
    table = etl.convert(table, 'mime_type', lambda v, row: 'application/xml' if row.id == 'fmt/979' else v, pass_row=True)

    if os.path.isfile(txt_target_path):
        os.remove(txt_target_path)

    data = etl.dicts(table)
    count = 0
    for row in data:
        count += 1
        source_file_path = row['source_file_path']
        source_file_path = os.path.join(source_dir, source_file_path)

        mime_type = row['mime_type'].split(';')[0]
        result = None

        if not mime_type:
            if os.path.islink(source_file_path):
                mime_type = 'n/a'

            if os.path.splitext(source_file_path)[1].lower() == '.xml':
                mime_type = 'application/xml'

        if not zipped:
            print_path = os.path.relpath(source_file_path, Path(source_dir).parents[1])
            count_str = ('(' + str(count) + '/' + str(file_count) + '): ')
            print(count_str + '.../' + print_path + ' (' + mime_type + ')')

        target_file_dir = os.path.dirname(source_file_path.replace(source_dir, target_dir))
        origfile = File(source_file_path, mime_type, row['version'])
        normalized = origfile.convert(target_file_dir, None)

        result = normalized['msg']

        if result == 'Manually converted':
            if row['result'] not in ('Converted successfully', 'Manually converted'):
                converted_now = True
            else:
                result = row['result']

        if result in ('Conversion failed', 'Conversion not supported'):
            errors = True
            result_file.append_txt(result + ': ' + source_file_path + ' (' + mime_type + ')')

        if result == 'Converted successfully':
            converted_now = True

        if normalized['norm_file_path']:
            row['norm_file_path'] = relpath(normalized['norm_file_path'], target_dir)

        file_copy_path = normalized['original_file_copy']
        if file_copy_path:
            file_copy_path = relpath(file_copy_path, target_dir)
        row['original_file_copy'] = file_copy_path

        row['result'] = result
        row_values = list(row.values())

        # TODO: Fikset med å legge inn escapechar='\\' i append_tsv_row -> vil det skal problemer senere?
        # row_values = [r.replace('\n', ' ') for r in row_values if r is not None]
        tsv_file.append_tsv_row(row_values)

    if len(os.listdir(tmp_dir)) == 0:
        os.rmdir(tmp_dir)

    shutil.move(tsv_target_path, tsv_source_path)
    # TODO: Legg inn valg om at hvis merge = true kopieres alle filer til mappe på øverste nivå og så slettes tomme undermapper

    msg = None
    if converted_now:
        msg = 'All files converted succcessfully.'
        if errors:
            msg = "Not all files were converted. See '" + txt_target_path + "' for details."
    else:
        msg = 'All files converted previously.'

    return msg, file_count, errors, originals  # TODO: Fiks så bruker denne heller for oppsummering til slutt når flere mapper konvertert

if __name__ == "__main__":
    typer.run(convert_folder)
