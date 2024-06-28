from pyspark import SparkContext, SparkConf
import re

# Initialize Spark
conf = SparkConf().setAppName("Better_Inverted_Index")
sc = SparkContext(conf=conf)

# Input path in HDFS
hdfs_input = 'hdfs://<HDFS_HOST>:<PORT>/path/to/input'

# Read input files and create a pair RDD (filename, content)
text_files = sc.wholeTextFiles(hdfs_input)

# Split text into words and count occurrences for each file
word_counts = text_files.flatMap(lambda filename_content: [(word.lower(), filename_content[0].split("/")[-1]) for word in re.findall(r'\b[A-Za-z]+\b', filename_content[1])])

# Group by word and document, and count the occurrences
word_doc_counts = word_counts.map(lambda word_file: (word_file, 1)).reduceByKey(lambda x, y: x + y)

# Group by word and sort documents by count and filename
word_sorted_docs = word_doc_counts.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey()

# Sort the documents for each word
def sort_documents(documents):
    sorted_documents = sorted(documents, key=lambda x: (-x[1], x[0]))
    return sorted_documents

sorted_word_docs = word_sorted_docs.mapValues(sort_documents)

# Get the first 5 results
output = sorted_word_docs.take(5)

# Print the results
for (word, documents) in output:
    document_strings = [f"{word_count} {doc}" for doc, word_count in documents]
    documents_line = ', '.join(document_strings)
    print(f"{word} {documents_line}")

# Output path in HDFS
hdfs_output= 'hdfs://<HDFS_HOST>:<PORT>/path/to/output'

# Save the output as text files
output_bii = sc.parallelize(output)
output_bii.saveAsTextFile(hdfs_output) 

# Stop Spark
sc.stop()