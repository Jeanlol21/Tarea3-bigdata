from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col

# Crear sesión Spark
spark = SparkSession.builder.appName("BatchAmazonReviews").getOrCreate()

# Cargar CSV
df = spark.read.csv("amazon_reviews.csv", header=True, inferSchema=True)

# Limpieza: eliminar nulos
df_clean = df.na.drop()

# Transformación: convertir texto a minúsculas
df_clean = df_clean.withColumn("review_body", lower(col("review_body")))

# Análisis: conteo por calificación
df_clean.groupBy("star_rating").count().show()

# Guardar resultados
df_clean.write.mode("overwrite").parquet("resultados_batch")

spark.stop()