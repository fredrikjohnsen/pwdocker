import os
import shutil
from ezdxf.addons import odafc
import typer

def dwg2dxf(src_path: str, dest_path: str):

    # Convert to temp folder to avoid problems with chmod
    # if dest_path is on Windows
    tmp_path = '/tmp/file.dxf'
    if os.path.exists(tmp_path):
       os.remove(tmp_path) 

    odafc.convert(src_path, tmp_path, version='R2018')
    shutil.copyfile(tmp_path, dest_path)


if __name__ == "__main__":
    typer.run(dwg2dxf)
