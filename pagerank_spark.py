from pyspark.sql import SparkSession

def parse_neighbors(line):
    parts = line.split()
    return parts[0], parts[1]

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

if __name__ == "__main__":
    # Crear la SparkSession
    spark = SparkSession.builder.appName("PageRank").getOrCreate()

    # Leer el archivo de enlaces
    lines = spark.sparkContext.textFile("graph.txt")

    # Parsear el archivo para extraer los enlaces
    links = lines.map(parse_neighbors).distinct().groupByKey().cache()

    # Inicializar los ranks
    ranks = links.mapValues(lambda _: 1.0)

    # Ejecutar 10 iteraciones del algoritmo de PageRank
    for iteration in range(10):
        # Calcular contribuciones
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])
        )
        # Actualizar ranks
        ranks = contribs.reduceByKey(lambda x, y: x + y).mapValues(lambda rank: 0.15 + 0.85 * rank)

    # Mostrar los resultados
    with open('part-000002' , "w") as file:
        for (link, rank) in ranks.collect():
            file.write(f"{link}\t{rank:.2f}\n")
    # Finalizar la sesión de Spark
    spark.stop()

