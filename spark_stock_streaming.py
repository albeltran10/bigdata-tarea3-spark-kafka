from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum, count, round
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType

# -------------------------------------------------------------------------
# 1. INICIALIZACIÓN DE LA SESIÓN DE SPARK STREAMING
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("SP500_Kafka_Streaming_Analytics") \
    .getOrCreate()

# Reducir logs ruidosos para observar la consola con claridad
spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------------------------------
# 2. DEFINICIÓN DEL ESQUEMA DEL EVENTO BURSÁTIL
# -------------------------------------------------------------------------
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

# -------------------------------------------------------------------------
# 3. LECTURA CONTINUA DESDE APACHE KAFKA
# -------------------------------------------------------------------------
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stock_ticks") \
    .option("startingOffsets", "latest") \
    .load()

# -------------------------------------------------------------------------
# 4. PARSEO DE JSON Y ESTRUCTURACIÓN DE CAMPOS
# -------------------------------------------------------------------------
parsed_stream = raw_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# -------------------------------------------------------------------------
# 5. AGREGACIONES EN VENTANAS TEMPORALES EN TIEMPO REAL
# -------------------------------------------------------------------------
# Ventanas fijas de 30 segundos agrupando por Ticker
windowed_analytics = parsed_stream \
    .groupBy(
        window(col("timestamp"), "30 seconds"),
        col("ticker")
    ) \
    .agg(
        round(avg("price"), 2).alias("avg_price_usd"),
        sum("volume").alias("total_volume"),
        count("ticker").alias("num_transactions")
    )

# -------------------------------------------------------------------------
# 6. ESCRITURA DE RESULTADOS EN CONSOLA (STREAMING SINK)
# -------------------------------------------------------------------------
query = windowed_analytics.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print(">>> [STREAMING] Consumiendo eventos desde Kafka y procesando ventanas...")
query.awaitTermination()
