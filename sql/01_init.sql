-- sql/01_init.sql
CREATE DATABASE IF NOT EXISTS pwconvert;
USE pwconvert;

-- Main files table
CREATE TABLE IF NOT EXISTS file (
    id INT AUTO_INCREMENT PRIMARY KEY,
    path VARCHAR(1000) NOT NULL,
    size BIGINT,
    mime VARCHAR(255),
    format VARCHAR(255),
    version VARCHAR(100),
    status ENUM('new', 'processing', 'converted', 'failed', 'accepted', 'skipped', 'protected', 'timeout', 'deleted', 'removed') DEFAULT 'new',
    puid VARCHAR(50),
    class VARCHAR(100),
    source_id INT,
    encoding VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    status_ts TIMESTAMP NULL,
    error_message TEXT,
    target_path VARCHAR(1000),
    kept BOOLEAN DEFAULT TRUE,
    original BOOLEAN DEFAULT TRUE,
    finished BOOLEAN DEFAULT FALSE,
    subpath VARCHAR(500),
    INDEX idx_status (status),
    INDEX idx_path (path(255)),
    INDEX idx_source_id (source_id),
    INDEX idx_puid (puid),
    INDEX idx_mime (mime)
);

-- CTE (Conversion Type Extensions) table for file type mappings
CREATE TABLE IF NOT EXISTS cte (
    id INT AUTO_INCREMENT PRIMARY KEY,
    extension VARCHAR(10) NOT NULL,
    mime_type VARCHAR(255) NOT NULL,
    puid VARCHAR(50),
    description TEXT,
    converter VARCHAR(100),
    target_extension VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_ext_mime (extension, mime_type)
);

-- Insert some common file type mappings
INSERT IGNORE INTO cte (extension, mime_type, puid, description, converter, target_extension) VALUES
('pdf', 'application/pdf', 'fmt/276', 'Portable Document Format', 'copy', 'pdf'),
('doc', 'application/msword', 'fmt/40', 'Microsoft Word Document', 'libreoffice', 'pdf'),
('docx', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 'fmt/412', 'Microsoft Word Document (OpenXML)', 'libreoffice', 'pdf'),
('xls', 'application/vnd.ms-excel', 'fmt/61', 'Microsoft Excel Spreadsheet', 'libreoffice', 'pdf'),
('xlsx', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'fmt/214', 'Microsoft Excel Spreadsheet (OpenXML)', 'libreoffice', 'pdf'),
('ppt', 'application/vnd.ms-powerpoint', 'fmt/126', 'Microsoft PowerPoint Presentation', 'libreoffice', 'pdf'),
('pptx', 'application/vnd.openxmlformats-officedocument.presentationml.presentation', 'fmt/215', 'Microsoft PowerPoint Presentation (OpenXML)', 'libreoffice', 'pdf'),
('txt', 'text/plain', 'x-fmt/111', 'Plain Text', 'copy', 'txt'),
('rtf', 'application/rtf', 'fmt/355', 'Rich Text Format', 'libreoffice', 'pdf'),
('odt', 'application/vnd.oasis.opendocument.text', 'fmt/291', 'OpenDocument Text', 'libreoffice', 'pdf'),
('ods', 'application/vnd.oasis.opendocument.spreadsheet', 'fmt/295', 'OpenDocument Spreadsheet', 'libreoffice', 'pdf'),
('odp', 'application/vnd.oasis.opendocument.presentation', 'fmt/293', 'OpenDocument Presentation', 'libreoffice', 'pdf');

-- Conversion log table
CREATE TABLE IF NOT EXISTS conversion_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_id INT,
    source_path VARCHAR(1000),
    target_path VARCHAR(1000),
    status VARCHAR(50),
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    duration_seconds INT,
    error_message TEXT,
    converter_used VARCHAR(100),
    FOREIGN KEY (file_id) REFERENCES file(id) ON DELETE CASCADE
);