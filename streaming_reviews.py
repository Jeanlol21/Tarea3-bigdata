from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Crear sesión Spark
spark = SparkSession.builder.appName("KafkaStreamingReviews").getOrCreate()

# Leer desde Kafka
df_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reviews_topic").load()

# Convertir a texto
reviews = df_stream.selectExpr("CAST(value AS STRING)")

# Dividir en palabras
words = reviews.select(explode(split(reviews.value, " ")).alias("word"))

# Contar palabras
wordCounts = words.groupBy("word").count()

# Mostrar resultados en consola
query = wordCounts.writeStream.outputMode("complete") \
    .format("console").start()

query.awaitTermination()