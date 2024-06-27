-- Crear la base de datos si no existe
CREATE DATABASE IF NOT EXISTS pagerank;
USE pagerank;

-- Crear y cargar la tabla page_links
DROP TABLE IF EXISTS page_links;
CREATE EXTERNAL TABLE page_links (
  origin STRING,
  destination STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://ip-172-31-26-142.ec2.internal:8020/data';

-- Crear tabla para las calificaciones iniciales
DROP TABLE IF EXISTS page_ranks;
CREATE TABLE page_ranks AS
SELECT DISTINCT origin AS page, 1.0 AS rank
FROM page_links;

-- Crear tabla para las contribuciones
DROP TABLE IF EXISTS page_contributions;
CREATE TABLE page_contributions (
  page STRING,
  rank DOUBLE
);

-- Crear tabla para las calificaciones finales
DROP TABLE IF EXISTS page_final_ranks;
CREATE TABLE page_final_ranks (
  page STRING,
  rank DOUBLE
);

-- Inicializar la tabla page_ranks
INSERT OVERWRITE TABLE page_ranks
SELECT DISTINCT origin AS page, 1.0 AS rank
FROM page_links;

-- Número de iteraciones
SET iterations = 10;

-- Factor de amortiguamiento
SET damping_factor = 0.85;

-- Número total de páginas (calcular si es necesario)
SET total_pages = (SELECT COUNT(DISTINCT page) FROM page_ranks);

-- Iterar para calcular el PageRank
BEGIN
  DECLARE iteration INT;
  SET iteration = 1;

  WHILE (iteration <= ${iterations}) DO
    -- Calcular contribuciones a cada página
    INSERT OVERWRITE TABLE page_contributions
    SELECT
      destination AS page,
      SUM(rank / COUNT(destination) OVER (PARTITION BY origin)) AS rank
    FROM page_links
    JOIN page_ranks ON page_links.origin = page_ranks.page
    GROUP BY destination;

    -- Actualizar las calificaciones con el factor de amortiguamiento
    INSERT OVERWRITE TABLE page_ranks
    SELECT
      page,
      (1 - ${damping_factor}) / ${total_pages} + ${damping_factor} * COALESCE(rank, 0)
    FROM (
      SELECT page FROM page_ranks
      UNION
      SELECT destination AS page FROM page_links
    ) AS pages
    LEFT JOIN page_contributions ON pages.page = page_contributions.page;

    SET iteration = iteration + 1;
  END WHILE;
END;

-- Guardar las calificaciones finales en page_final_ranks
INSERT OVERWRITE TABLE page_final_ranks
SELECT * FROM page_ranks;

-- Exportar los resultados de la tabla page_final_ranks a un archivo en Amazon S3
INSERT OVERWRITE DIRECTORY 'hdfs://ip-172-31-26-142.ec2.internal:8020/user/hadoop/logs'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT * FROM page_final_ranks
ORDER BY rank DESC;
