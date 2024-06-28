from pyspark import SparkContext, SparkConf
import re

# Inicializar Spark
conf = SparkConf().setAppName("Better_Inverted_Index")
sc = SparkContext(conf=conf)

# Ruta de entrada en HDFS
hdfs_input = 'hdfs://ip-172-31-25-107.ec2.internal:8020/corpus'

# Leer archivos de entrada y crear un RDD de pares (nombre de archivo, contenido)
text_files = sc.wholeTextFiles(hdfs_input)

# Dividir el texto en palabras y contar ocurrencias por archivo
word_counts = text_files.flatMap(lambda filename_content: [(word.lower(), filename_content[0].split("/")[-1]) for word in re.findall(r'\b[A-Za-z]+\b', filename_content[1])])

print(word_counts)

# Agrupar por palabra y documento, y contar las ocurrencias
word_doc_counts = word_counts.map(lambda word_file: (word_file, 1)).reduceByKey(lambda x, y: x + y)

# Agrupar por palabra y ordenar documentos por cuenta y nombre de archivo
word_sorted_docs = word_doc_counts.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey()

# Ordenar los documentos para cada palabra
def sort_documents(documents):
    sorted_documents = sorted(documents, key=lambda x: (-x[1], x[0]))
    return sorted_documents

sorted_word_docs = word_sorted_docs.mapValues(sort_documents)

# Obtener los primeros 5 resultados
output = sorted_word_docs.take(100)

# Formatea los resultados para guardar en HDFS
lines_to_save = []
for (word, documents) in output:
    for doc, word_count in documents:
        line = f"{word}\t{hdfs_input}/{doc}"
        lines_to_save.append(line)
#    document_strings = [f"{word_count} {doc}" for doc, word_count in documents]
#    documents_line = ', '.join(document_strings)
   # print(f"{word} {documents_line}")

# Ruta de salida en HDFS
hdfs_output = 'hdfs://ip-172-31-25-107.ec2.internal:8020/user/hadoop/logs_spark3'

# Guardar la salida como archivos de texto
output_bii = sc.parallelize(lines_to_save)
output_bii.saveAsTextFile(hdfs_output) 

# Detener Spark
sc.stop()

