# Python conversion project

![Pw-Convert](https://i.imgur.com/hDoBZuk.png)

contains scripts to convert files of different formats to archivable formats specified
in this link https://www.digdir.no/standarder/arkivstandarder/1482

# Install for WSL

1. Start WSL
     ```sh
   wsl.exe
   ```
  * Supported distros:
    * Ubuntu
    * Uma
    * Ulyana
    * Focal
    * Jammy
    * Ulyssa
    * Una
    * Vanessa
    * Virginia
2. Clone repo to desired location
   ```sh
   git clone https://github.com/Preservation-Workbench/PWConvert
   ```
3. cd into cloned repo
4. Make sure Python is installed, with pip package installer.
   * Install Python
   ```sh
   sudo apt install python3
   ```
   * Install pip
   ```sh
   sudo apt install python3-pip
   ```
   ```sh
   python3 --version  # Check Python version
   pip3 --version     # Check pip version
   ```
5. Install packages listet in requirements.txt
   ```sh
   pip install -r requirements.txt
   ```
6. Run install-script
   ```sh
   sudo ./install.sh
   ```
7. Start program
   ```sh
   python3 convert.py
   ```

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
