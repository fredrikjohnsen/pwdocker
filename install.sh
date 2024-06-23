#!/bin/bash
export DEBIAN_FRONTEND=noninteractive
SCRIPTPATH=$(dirname $(readlink -f "${BASH_SOURCE[0]}" 2>/dev/null||echo $0))
OWNER=$(stat -c '%U' $SCRIPTPATH)
UPDATE=false
EMAILCONVERTPATH=/home/$OWNER/bin/emailconvert
# USERID=$(id -u $OWNER)
# export DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/$USERID/bus";

cecho(){
    RED="\033[1;31m"
    GREEN="\033[1;32m"  # <-- [0 is not bold
    YELLOW="\033[1;33m" # <-- [1 is bold
    CYAN="\033[1;36m"
    NC="\033[0m" # No Color
    printf "${!1}${2} ${NC}\n";
}

recho(){
    if [[ $? -eq 0 ]]; then
        cecho "GREEN" "Done!"
    else
        cecho "RED" "Operation failed. Exiting script.."; exit 1;
    fi
}

if [ "$EUID" -ne 0 ]; then
    cecho "RED" "Please run as root!"; exit 1;
fi

DISTRO=$(lsb_release -sc) # Get distro codename
if [[ "${DISTRO}" != @(uma|ulyana|focal|jammy|Ulyssa|una|vanessa|virginia) ]]; then
    cecho "RED" "Distro not supported. Exiting script.."; exit 1;
fi

cecho "CYAN" "Installing script essentials if missing..";
dpkg -s curl 2>/dev/null >/dev/null || apt-get -y install curl;
dpkg -s ca-certificates 2>/dev/null >/dev/null || apt-get -y install ca-certificates;
dpkg -s apt-transport-https 2>/dev/null >/dev/null || apt-get -y install apt-transport-https;
recho $?;

if [[ "$UPDATE" = true ]]; then
    cecho "CYAN" "Updating repo info..";
    apt-get update;
    recho $?;
fi

cecho "CYAN" "Installing apt-gettable dependencies..";
echo ttf-mscorefonts-installer msttcorefonts/accepted-mscorefonts-eula select true | debconf-set-selections;
apt-get install -y ttf-mscorefonts-installer pandoc abiword sqlite3 uchardet \
    libreoffice python3-wheel tesseract-ocr ghostscript unar texlive-latex-extra \
    icc-profiles-free clamtk  php-cli wkhtmltopdf texlive-xetex librsvg2-bin \
    ruby-dev  imagemagick cabextract dos2unix libclamunrar9 wimtools vlc \
    fontforge python3-pgmagick graphicsmagick graphviz img2pdf golang \
    php-xml libtiff-tools xvfb;
recho $?;

cecho "CYAN" "Installing onlyoffice-documentbuilder"
curl -LOs https://github.com/ONLYOFFICE/DocumentBuilder/releases/download/v8.0.0/onlyoffice-documentbuilder_amd64.deb && apt install ./onlyoffice-documentbuilder_amd64.deb && rm -f onlyoffice-documentbuilder_amd64.deb;

cecho "CYAN" "Installing Siegfried"
curl -LOs https://github.com/richardlehane/siegfried/releases/download/v1.11.0/siegfried_1.11.0-1_amd64.deb && apt install ./siegfried_1.11.0-1_amd64.deb && rm -f siegfried_1.11.0-1_amd64.deb

cecho "CYAN" "Install mail converter..";
gem install nokogiri -v 1.15.6;
gem install net-imap -v 0.3.7;
gem install eml_to_pdf;
recho $?;

cecho "CYAN" "Installing java email converter..";
if [ ! -f $EMAILCONVERTPATH/emailconverter.jar ]; then
    mkdir -p $EMAILCONVERTPATH;
    curl -o $EMAILCONVERTPATH/emailconverter.jar -L \
    https://github.com/nickrussler/email-to-pdf-converter/releases/download/2.5.3/emailconverter-2.5.3-all.jar;
    recho $?;  
fi

cecho "CYAN" "Installing ODAFileConverter..";
curl -LOs https://download.opendesign.com/guestfiles/Demo/ODAFileConverter_QT6_lnxX64_8.3dll_25.5.deb && apt install ./ODAFileConverter_QT6_lnxX64_8.3dll_25.5.deb && rm -f ODAFileConverter_QT6_lnxX64_8.3dll_25.5.deb;
recho $?;

cecho "CYAN" "Fix fuse permissions..";
sed -i -e 's/#user_allow_other/user_allow_other/' /etc/fuse.conf;
recho $?;

if [ -f "/etc/ImageMagick-6/policy.xml" ]; then
    cecho "CYAN" "Fix pdf permissions for ImageMagick.."
    mv /etc/ImageMagick-6/policy.xml /etc/ImageMagick-6/policy.xmlout 2>/dev/null;
    recho $?;
fi

if [ $(fc-list | grep -c Calibri) -eq 0 ]; then
    cecho "CYAN" "Installing microsoft fonts..";
    export ACCEPT_EULA=true;
    curl -Ls https://raw.githubusercontent.com/metanorma/vista-fonts-installer/master/vista-fonts-installer.sh | bash;
    recho $?;
fi

