from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, monotonically_increasing_id, collect_list

# Inicializar Spark
spark = SparkSession.builder \
    .appName("Inverted Index") \
    .getOrCreate()

# Leer datos desde un archivo de texto
docs = spark.read.text("path/to/documents.txt")

# Añadir un identificador único para cada línea
docs_with_id = docs.withColumn("doc_id", monotonically_increasing_id())

# Dividir cada línea en palabras
words = docs_with_id.select(
    explode(split(docs_with_id["value"], " ")).alias("word"),
    docs_with_id["doc_id"]
)

# Agrupar por palabra y coleccionar los identificadores de documentos en una lista
inverted_index = words.groupBy("word").agg(collect_list("doc_id").alias("doc_ids"))

# Guardar el índice invertido en un archivo Parquet
inverted_index.write.mode("overwrite").parquet("path/to/inverted_index.parquet")

# Parar Spark
spark.stop()