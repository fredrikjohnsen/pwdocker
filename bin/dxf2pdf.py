import os
import shutil
import ezdxf
from ezdxf.addons import odafc
from ezdxf.addons.drawing import Frontend, RenderContext
from ezdxf.addons.drawing import layout, pymupdf, config
import typer

def dwg2pdf(src_path: str, dest_path: str, dark_bg: bool=False):

    doc = ezdxf.readfile(src_path)
    msp = doc.modelspace()

    # 1. create the render context
    context = RenderContext(doc)
    # 2. create the backend
    backend = pymupdf.PyMuPdfBackend()
    # 3. create the frontend
    if not dark_bg:
        cfg = config.Configuration(background_policy=config.BackgroundPolicy.WHITE)
        frontend = Frontend(context, backend, config=cfg)
    else:
        frontend = Frontend(context, backend)
    # 4. draw the modelspace
    frontend.draw_layout(msp)
    # 5. create an A4 page layout
    page = layout.Page(210, 297, layout.Units.mm, margins=layout.Margins.all(20))
    # 6. get the PDF rendering as bytes
    backend.get_pdf_bytes(page)

    with open(dest_path, "wb") as fp:
        fp.write(backend.get_pdf_bytes(layout.Page(0, 0)))

if __name__ == "__main__":
    typer.run(dwg2pdf)
