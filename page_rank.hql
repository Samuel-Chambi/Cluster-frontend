CREATE DATABASE IF NOT EXISTS pagerank;
USE pagerank;

DROP TABLE IF EXISTS page_links;
CREATE EXTERNAL TABLE page_links (
  origin STRING,
  destination STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';
LOAD DATA INPATH 'hdfs://ip-172-31-26-142.ec2.internal:8020/data/graph.txt'
OVERWRITE INTO TABLE page_links;

-- Table for initial ranks
DROP TABLE IF EXISTS page_ranks;
CREATE TABLE page_ranks AS
SELECT DISTINCT origin AS page, 1.0 AS rank
FROM page_links;

-- Table for contributions
DROP TABLE IF EXISTS page_contributions;
CREATE TABLE page_contributions (
  page STRING,
  rank DOUBLE
);

-- Table for final ranks
DROP TABLE IF EXISTS page_final_ranks;
CREATE TABLE page_final_ranks (
  page STRING,
  rank DOUBLE
);

INSERT OVERWRITE TABLE page_ranks
SELECT DISTINCT origin AS page, 1.0 AS rank
FROM page_links;

-- Number of iterations
SET iterations = 10;

-- Damping factor
SET damping_factor = 0.85;

-- Total number of pages (calculate if necessary)
SET total_pages = (SELECT COUNT(DISTINCT page) FROM page_ranks);

-- Iterate
FOR (i = 1; i <= ${iterations}; i++) DO
  -- Calculate contributions to each page
  INSERT OVERWRITE TABLE page_contributions
  SELECT
    destination AS page,
    SUM(rank / COUNT(destination) OVER (PARTITION BY origin)) AS rank
  FROM page_links
  JOIN page_ranks ON page_links.origin = page_ranks.page
  GROUP BY destination;

  -- Update ranks with damping factor
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
END;

INSERT OVERWRITE TABLE page_final_ranks
SELECT * FROM page_ranks;

-- Exportar los resultados de la tabla page_final_ranks a un archivo en Amazon S3
INSERT OVERWRITE DIRECTORY 'hdfs://ip-172-31-26-142.ec2.internal:8020/user/hadoop/logs'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT * FROM page_final_ranks
ORDER BY rank DESC;

