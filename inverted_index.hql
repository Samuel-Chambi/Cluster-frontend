-- Crear la base de datos si no existe
CREATE DATABASE IF NOT EXISTS inverted_index;
USE inverted_index;

-- Crear la tabla externa para leer archivos desde el directorio
DROP TABLE IF EXISTS external_docs;
CREATE EXTERNAL TABLE external_docs(line STRING)
LOCATION 's3://aws-logs-888525559191-us-east-1/corpus';

-- Crear la tabla 'docs' con columnas para file_id, line_number y contenido de la línea
DROP TABLE IF EXISTS docs;
CREATE TABLE docs (
    file_id STRING,
    line_number INT,
    line STRING
);

-- Insertar datos en la tabla 'docs'
INSERT INTO docs
SELECT
    INPUT__FILE__NAME AS file_id,
    ROW_NUMBER() OVER (PARTITION BY INPUT__FILE__NAME ORDER BY INPUT__FILE__NAME) AS line_number,
    line
FROM external_docs;

-- Crear la tabla 'inverted_index'
DROP TABLE IF EXISTS inverted_index;
CREATE TABLE inverted_index AS
SELECT word, file_id
FROM (
    SELECT file_id,
           line,
           word
    FROM docs
    LATERAL VIEW explode(split(regexp_replace(line, '[^a-zA-Z]', ' '), '\\s+')) exploded_docs AS word
) exploded_docs
GROUP BY word, file_id
ORDER BY word, file_id;

-- Guardar la salida de 'inverted_index' en S3
INSERT OVERWRITE DIRECTORY 'hdfs://ip-172-31-18-61.ec2.internal:8020/user/hadoop/logs'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT * FROM inverted_index;