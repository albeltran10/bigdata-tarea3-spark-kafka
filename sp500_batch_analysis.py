import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, round, lag, when, 
    avg, stddev, max, min, sum, year
)
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import seaborn as sns

# -------------------------------------------------------------------------
# 1. INICIALIZACIÓN DE LA SESIÓN DE SPARK
# -------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("SP500_Batch_Analytics") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Reducir verbosidad de logs en consola
spark.sparkContext.setLogLevel("WARN")

print(">>> [INFO] Sesión de Apache Spark inicializada con éxito.")

# -------------------------------------------------------------------------
# 2. CARGA DEL CONJUNTO DE DATOS (BATCH)
# -------------------------------------------------------------------------
# Ajustar la ruta al archivo CSV descargado de Kaggle
csv_path = "all_stocks_5yr.csv"

raw_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_path)

print(f">>> [INFO] Esquema original inferido:")
raw_df.printSchema()

total_registros_iniciales = raw_df.count()
print(f">>> [INFO] Total de registros leídos en batch: {total_registros_iniciales}")

# -------------------------------------------------------------------------
# 3. LIMPIEZA Y TRANSFORMACIÓN DE TIPOS DE DATOS
# -------------------------------------------------------------------------
# Normalizar nombres de columnas, parsear fecha y eliminar valores nulos críticos
clean_df = raw_df \
    .withColumnRenamed("Name", "Ticker") \
    .withColumn("Date", to_date(col("Date"), "yyyy-MM-dd")) \
    .filter(
        col("Ticker").isNotNull() & 
        col("Close").isNotNull() & 
        col("Volume").isNotNull() &
        (col("Volume") > 0)
    )

clean_df.cache()
total_limpios = clean_df.count()
print(f">>> [INFO] Registros válidos tras limpieza: {total_limpios} (Descartados: {total_registros_iniciales - total_limpios})")

# -------------------------------------------------------------------------
# 4. INGENIERÍA DE CARACTERÍSTICAS Y FUNCIONES DE VENTANA (WINDOWING)
# -------------------------------------------------------------------------
# Definición de especificación de ventana particionada por Ticker y ordenada por Fecha
ticker_window = Window.partitionBy("Ticker").orderBy("Date")

# Ventana móvil de 20 y 50 sesiones para cálculo de medias móviles simples (SMA)
sma20_window = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-19, 0)
sma50_window = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-49, 0)

transformed_df = clean_df \
    .withColumn("Prev_Close", lag("Close", 1).over(ticker_window)) \
    .withColumn(
        "Daily_Return_Pct",
        when(col("Prev_Close").isNotNull(),
             round(((col("Close") - col("Prev_Close")) / col("Prev_Close")) * 100, 4)
        ).otherwise(0.0)
    ) \
    .withColumn("Daily_Spread_USD", round(col("High") - col("Low"), 2)) \
    .withColumn("SMA_20", round(avg("Close").over(sma20_window), 2)) \
    .withColumn("SMA_50", round(avg("Close").over(sma50_window), 2)) \
    .withColumn("Year", year(col("Date")))

print(">>> [INFO] Muestra de datos transformados con indicadores técnicos:")
transformed_df.select("Ticker", "Date", "Close", "Daily_Return_Pct", "SMA_20", "SMA_50").show(10)

# -------------------------------------------------------------------------
# 5. ANÁLISIS EXPLORATORIO DE DATOS (EDA) DISTRIBUIDO
# -------------------------------------------------------------------------
print(">>> [EDA 1] Top 10 activos con mayor volumen medio diario negociado:")
top_volume_df = transformed_df.groupBy("Ticker") \
    .agg(
        round(avg("Volume"), 0).alias("Avg_Daily_Volume"),
        round(avg("Close"), 2).alias("Avg_Price_USD"),
        round(sum("Volume"), 0).alias("Total_Volume")
    ) \
    .orderBy(col("Avg_Daily_Volume").desc())

top_volume_df.show(10)

print(">>> [EDA 2] Top 10 activos con mayor volatilidad histórica (Desviación estándar de retornos):")
volatility_df = transformed_df.groupBy("Ticker") \
    .agg(
        round(stddev("Daily_Return_Pct"), 4).alias("Return_Volatility_StdDev"),
        round(avg("Daily_Return_Pct"), 4).alias("Avg_Daily_Return_Pct")
    ) \
    .filter(col("Return_Volatility_StdDev").isNotNull()) \
    .orderBy(col("Return_Volatility_StdDev").desc())

volatility_df.show(10)

print(">>> [EDA 3] Consolidado anual de volumen transaccional en el índice:")
yearly_summary_df = transformed_df.groupBy("Year") \
    .agg(
        round(sum("Volume"), 0).alias("Total_Annual_Volume"),
        round(avg("Daily_Return_Pct"), 4).alias("Market_Avg_Daily_Return")
    ) \
    .orderBy("Year")

yearly_summary_df.show()

# -------------------------------------------------------------------------
# 6. VISUALIZACIÓN DE RESULTADOS BATCH (EXTRACCIÓN SEGURA A PANDAS)
# -------------------------------------------------------------------------
# Regla de oro en Big Data: Solo se pasa a Pandas el resultado de agregaciones,
# nunca el dataset distribuido crudo.

# A. Gráfico de Barras: Top 10 Empresas por Volumen Promedio
top10_vol_pd = top_volume_df.limit(10).toPandas()

plt.figure(figsize=(10, 5))
sns.barplot(data=top10_vol_pd, x="Ticker", y="Avg_Daily_Volume", palette="viridis")
plt.title("Top 10 Activos del S&P 500 por Volumen Promedio Diario Negociado", fontsize=12, fontweight='bold')
plt.xlabel("Ticker Bursátil", fontsize=10)
plt.ylabel("Volumen Promedio Diario (Acciones)", fontsize=10)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.savefig("grafico_1_top_volumen.png", dpi=300)
plt.close()
print(">>> [INFO] Gráfico 1 exportado: 'grafico_1_top_volumen.png'")

# B. Gráfico de Serie Temporal: Precios de Cierre y Medias Móviles (Caso Apple - AAPL)
aapl_pd = transformed_df.filter(col("Ticker") == "AAPL") \
    .select("Date", "Close", "SMA_20", "SMA_50") \
    .orderBy("Date") \
    .toPandas()

plt.figure(figsize=(12, 6))
plt.plot(aapl_pd["Date"], aapl_pd["Close"], label="Precio de Cierre (USD)", color="black", alpha=0.6, linewidth=1)
plt.plot(aapl_pd["Date"], aapl_pd["SMA_20"], label="SMA 20 días (Corto Plazo)", color="blue", linewidth=1.2)
plt.plot(aapl_pd["Date"], aapl_pd["SMA_50"], label="SMA 50 días (Mediano Plazo)", color="orange", linewidth=1.2)
plt.title("Serie Temporal y Cruce de Medias Móviles: Apple Inc. (AAPL)", fontsize=12, fontweight='bold')
plt.xlabel("Fecha", fontsize=10)
plt.ylabel("Precio de la Acción (USD)", fontsize=10)
plt.legend(loc="upper left")
plt.grid(True, linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig("grafico_2_serie_aapl_sma.png", dpi=300)
plt.close()
print(">>> [INFO] Gráfico 2 exportado: 'grafico_2_serie_aapl_sma.png'")

# C. Histograma de Distribución de Retornos Diarios
aapl_returns_pd = transformed_df.filter(col("Ticker") == "AAPL") \
    .select("Daily_Return_Pct") \
    .filter(col("Daily_Return_Pct") != 0.0) \
    .toPandas()

plt.figure(figsize=(9, 5))
sns.histplot(aapl_returns_pd["Daily_Return_Pct"], bins=50, kde=True, color="darkcyan")
plt.title("Distribución de Retornos Diarios (%): Apple Inc. (AAPL)", fontsize=12, fontweight='bold')
plt.xlabel("Retorno Diario (%)", fontsize=10)
plt.ylabel("Frecuencia", fontsize=10)
plt.grid(axis='y', linestyle='--', alpha=0.5)
plt.tight_layout()
plt.savefig("grafico_3_distribucion_retornos.png", dpi=300)
plt.close()
print(">>> [INFO] Gráfico 3 exportado: 'grafico_3_distribucion_retornos.png'")

# Finalizar la sesión
spark.stop()
print(">>> [INFO] Procesamiento Batch finalizado y recursos liberados correctamente.")
