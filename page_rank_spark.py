from pyspark.sql import SparkSession

# Inicializar Spark
spark = SparkSession.builder \
    .appName("PageRank") \
    .getOrCreate()

# Leer datos desde un archivo de texto
lines = spark.read.text("path/to/links.txt").rdd
links = lines.map(lambda line: tuple(line.value.split())).distinct().groupByKey().cache()

# Inicializar los ranks de cada página
ranks = links.mapValues(lambda _: 1.0)

# Definir la función para distribuir los ranks
def compute_contribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

# Ejecutar iteraciones para ajustar los ranks
for iteration in range(10):
    contribs = links.join(ranks).flatMap(lambda url_urls_rank: compute_contribs(url_urls_rank[1][0], url_urls_rank[1][1]))
    ranks = contribs.reduceByKey(lambda x, y: x + y).mapValues(lambda rank: 0.15 + 0.85 * rank)

# Mostrar los ranks finales de cada página
for link, rank in ranks.collect():
    print(f"{link}: {rank}")

# Parar Spark
spark.stop()