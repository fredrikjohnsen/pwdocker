FROM ubuntu:22.04

# Avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Set working directory
WORKDIR /app

# Install system dependencies including MySQL client
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-wheel \
    curl \
    ca-certificates \
    apt-transport-https \
    sqlite3 \
    mysql-client \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    pandoc \
    abiword \
    uchardet \
    libreoffice \
    tesseract-ocr \
    ghostscript \
    unar \
    texlive-latex-extra \
    icc-profiles-free \
    php-cli \
    wkhtmltopdf \
    texlive-xetex \
    librsvg2-bin \
    imagemagick \
    cabextract \
    dos2unix \
    fontforge \
    graphicsmagick \
    graphviz \
    img2pdf \
    php-xml \
    libtiff-tools \
    xvfb \
    fuse \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft fonts
RUN echo ttf-mscorefonts-installer msttcorefonts/accepted-mscorefonts-eula select true | debconf-set-selections \
    && apt-get update && apt-get install -y ttf-mscorefonts-installer \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Install MySQL Python connector
RUN pip3 install --no-cache-dir mysqlclient PyMySQL

# Install additional tools
RUN mkdir -p /tmp/downloads && cd /tmp/downloads \
    # Install pdfcpu
    && curl -L https://github.com/pdfcpu/pdfcpu/releases/download/v0.8.1/pdfcpu_0.8.1_Linux_x86_64.tar.xz -o pdfcpu.tar.xz \
    && tar -xf pdfcpu.tar.xz \
    && mv pdfcpu_0.8.1_Linux_x86_64/pdfcpu /usr/local/bin/ \
    # Install Siegfried
    && curl -L https://github.com/richardlehane/siegfried/releases/download/v1.11.0/siegfried_1.11.0-1_amd64.deb -o siegfried.deb \
    && dpkg -i siegfried.deb || apt-get install -f -y \
    && rm -rf /tmp/downloads

# Fix ImageMagick PDF policy
RUN if [ -f "/etc/ImageMagick-6/policy.xml" ]; then \
        mv /etc/ImageMagick-6/policy.xml /etc/ImageMagick-6/policy.xml.bak; \
    fi

# Fix fuse permissions (only if file exists)
RUN if [ -f "/etc/fuse.conf" ]; then \
        sed -i -e 's/#user_allow_other/user_allow_other/' /etc/fuse.conf; \
    fi

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /tmp/convert /app/data

# Set up volume for persistent data
VOLUME ["/app/data"]

# Expose port if needed (for future web interface)
EXPOSE 8000

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python3", "convert.py", "--help"]