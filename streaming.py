from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[10]", "NetworkWordCount")
ssc = StreamingContext(sc, 5)

# Create a DStream that will connect to hostname:port, like localhost:9090
#DStream: secuencia continua de RDDs (del mismo tipo) que representa un flujo continuo de datos
lines = ssc.socketTextStream("localhost", 9090)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate