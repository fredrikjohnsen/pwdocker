# Python conversion project

contains scripts to convert files of different formats to archivable formats specified
in this link https://www.digdir.no/standarder/arkivstandarder/1482

# Install (WSL)

1. Start WSL
  * Supported distros:
    * uma
    * Ulyana
    * Focal
    * jammy
    * Ulyssa
    * una
    * Vanessa
    * Virginia
2. Clone repo to desired location
   >git clone https://github.com/Preservation-Workbench/PWConvert
3. cd into cloned repo
4. Make sure Ptyhon is installed, with both pip package installer and npm package manager.
5. Install packages listet in requirements.txt
   >pip install -r requirements
6. Run install-script
   >sudo ./install.sh
7. Start program
   >python3 convert.py

# How to use

* Add your desired configuration to application.yml
* Make sure you have sqlite installed and the required python libraries
* Run convert.py
  * A database will now have been created in the directory specified in the configuration file
    * The file table contains an entry per file in the source directory and the conversion result
  * The converted files will now be located in the target directory
* The result will be printed to the console
  * More detailed results can be found in the file table

# Allowed standards

## Arkivdokumenter med ren tekst:

TXT, TIFF, PDF/A, XML

For presentasjoner i OOXML eller ODF format bør en PDF/A-versjon leveres som tillegg

## Arkivdokumenter inneholdende tekst med objekter

TIFF, PDF/A

## For digitale fotografier og bilder

TIFF, JPEG, PNG, PDF/A

## For kart

TIFF, SOSI, GML

## For videosekvenser

MPEG-2, MPEG-4/H.264

## For lydsekvenser

MP3, PCM-basert Wave, FLAC

## For regneark

PDF/A, XML

For presentasjoner i OOXML eller ODF format bør en PDF/A-versjon leveres som tillegg

## For web-sider

WARC, HTML, TIFF, PDF/A

## For presentasjoner

PDF/A, XML
For presentasjoner i OOXML eller ODF format bør en PDF/A-versjon leveres som tillegg

## For objektbaserte informasjonsmodeller for byggverk (BIM)

IFC

Pakking av data er tillatt etter følgende standard TAR – som spesifisert under standarden IEEE 1003.1
Komprimering av data er tillatt etter følgende standard ZIP – så lenge det er i henhold til ISO/IEC 21320-1
For andre typer formater må det gjøres særskilt avtale med Arkivverket
