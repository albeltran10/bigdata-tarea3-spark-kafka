# Tarea 3: Procesamiento de Grandes Volúmenes de Datos con Apache Spark y Apache Kafka

Repositorio correspondiente al componente práctico de la **Tarea 3: Procesamiento de Datos con Apache Spark** del curso **Big Data (202016911)** de la Universidad Nacional Abierta y a Distancia (UNAD).

Este proyecto implementa una solución de ingeniería de datos integral que aborda dos paradigmas de cómputo sobre series temporales del índice bursátil **S&P 500**:

1. **Procesamiento en Lote (Batch):** Ingesta, depuración, transformaciones analíticas avanzadas mediante funciones de ventana (*Window Functions*) y análisis exploratorio (EDA) distribuido utilizando la API de **PySpark DataFrames**.

2. **Procesamiento en Tiempo Real (Streaming):** Ingesta continua de transacciones de mercado mediante un productor distribuido en **Apache Kafka** y consumo/agregación en ventanas de tiempo con **Spark Structured Streaming**.

## 📁 Estructura del Repositorio

```
.
├── README.md                          # Documentación e instructivo de despliegue
├── all_stocks_5yr.csv                 # Conjunto de datos histórico (fuente batch)
├── sp500_batch_analysis.py            # Pipeline distribuido de procesamiento Batch
├── kafka_stock_producer.py            # Generador / Productor de eventos en tiempo real
├── spark_stock_streaming.py           # Pipeline de consumo y agregación con Spark Streaming
└── visualizations/                    # Resultados gráficos generados por el pipeline batch
    ├── grafico_1_top_volumen.png
    ├── grafico_2_serie_aapl_sma.png
    └── grafico_3_distribucion_retornos.png

```

## 🎯 Definición del Problema y Conjunto de Datos

### Contexto del Problema

El índice Standard & Poor's 500 (S&P 500) consolida el comportamiento de las 500 corporaciones más representativas del mercado accionario de EE. UU. Procesar series de tiempo bursátiles impone retos computacionales que superan las capacidades de herramientas convencionales de un solo hilo (como hojas de cálculo o scripts en Pandas puro):

* **Cómputo secuencial y dependencias cronológicas:** El cálculo de indicadores de dispersión, retornos y medias móviles (`SMA_20`, `SMA_50`) exige ordenar cronológicamente millones de registros preservando la localidad del dato por activo (`Ticker`) sin saturar el clúster con *shuffles* descontrolados.

* **Ingesta continua y latencia:** Las mesas de dinero y sistemas algorítmicos demandan evaluar precios promedio ponderados y volumen por minuto en tiempo real para reaccionar a picos de volatilidad.

### Dataset Utilizado

* **Nombre:** *S&P 500 Stock Historical Data* (Kaggle / IEX & Yahoo Finance).

* **Volumen:** Más de 619.000 observaciones registradas a nivel diario.

* **Variables clave:**

  * `Date`: Fecha de la jornada (`YYYY-MM-DD`).

  * `Ticker` / `Name`: Nemotécnico bursátil de la compañía (ej. AAPL, MSFT, TSLA, AMZN, BAC).

  * `Open`, `High`, `Low`, `Close`: Precios oficiales de la jornada en USD.

  * `Volume`: Número total de acciones transaccionadas en el día.

## 🛠️ Requisitos del Entorno

* **Sistema Operativo:** Servidor Linux (Ubuntu Server / Debian / WSL2).

* **Java:** OpenJDK 8 o 11 (`default-jre` / `default-jdk`).

* **Python:** Versión 3.8 o superior.

* **Apache Kafka:** Versión 3.x (con ZooKeeper clásico o modo KRaft).

* **Apache Spark:** Versión 3.x (con conector compatible `spark-sql-kafka-0-10`).

* **Librerías Python:**

  ```
  pip install pyspark kafka-python pandas matplotlib seaborn
  
  ```

## 🚀 Guía de Ejecución Paso a Paso

### Fase 1: Levantamiento de Servicios de Mensajería (Kafka & ZooKeeper)

1. **Iniciar el servicio de ZooKeeper** (en segundo plano):

   ```
   /opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
   
   ```

2. **Iniciar el Broker de Kafka**:

   ```
   /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
   
   ```

3. **Verificar que ambos procesos Java estén activos**:

   ```
   jps
   
   ```

   *Debe confirmar la existencia de `QuorumPeerMain` y `Kafka` en memoria.*

4. **Crear el topic para los eventos bursátiles**:

   ```
   /opt/kafka/bin/kafka-topics.sh --create --topic stock_ticks \
     --bootstrap-server localhost:9092 \
     --partitions 2 \
     --replication-factor 1
   
   ```

### Fase 2: Pipeline en Tiempo Real (Spark Structured Streaming & Kafka)

Para observar el flujo concurrente se deben utilizar dos terminales simultáneas:

* **Terminal 1 — Productor (Simulador de Mercado):**
  Lanza la generación de ticks bursátiles en formato JSON:

  ```
  python3 kafka_stock_producer.py
  
  ```

  *Comenzará a emitir transacciones con precio, volumen y timestamp cada segundo.*

* **Terminal 2 — Consumidor Distribuido en Spark:**
  Ejecuta la aplicación de streaming inyectando el paquete Maven de integración de Kafka:

  ```
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_stock_streaming.py
  
  ```

  *Spark procesará micro-lotes calculando precio promedio, volumen total acumulado y conteo de transacciones agrupadas en ventanas fijas de 30 segundos.*

### Fase 3: Pipeline por Lotes (Batch Analytics con PySpark)

1. Ubicar el archivo `all_stocks_5yr.csv` en el mismo directorio del script.

2. Ejecutar el procesamiento:

   ```
   python3 sp500_batch_analysis.py
   
   ```

3. **Flujo interno del procesamiento batch:**

   * Carga con tipado inferido y depuración de nulos/inconsistencias.

   * Implementación de **Window Functions** (`pyspark.sql.window.Window`) para estimar:

     * Retornos diarios porcentuales (`Daily_Return_Pct`) usando `lag("Close", 1)`.

     * Medias móviles simples de 20 días (`SMA_20`) y 50 días (`SMA_50`).

     * Rango de volatilidad diaria en USD (`High - Low`).

   * Generación automática de agregaciones EDA en consola (Top 10 activos por volumen, activos de mayor volatilidad histórica y volumen transaccional consolidado por año).

   * Generación y guardado de las 3 figuras PNG de alta resolución en disco:

     * `grafico_1_top_volumen.png`

     * `grafico_2_serie_aapl_sma.png`

     * `grafico_3_distribucion_retornos.png`

## 📊 Visualización de Resultados en Entornos de Servidor (Headless)

Dado que los scripts corren sobre un servidor sin entorno gráfico nativo, las figuras se guardan de forma aislada sin llamadas a `plt.show()`. Para descargar o previsualizar los resultados desde cualquier máquina cliente en la red:

```
# Levantar servidor HTTP temporal en la VM
python3 -m http.server 8080

```

Acceder vía navegador a: `http://<IP_DE_TU_VM>:8080/` y descargar las visualizaciones generadas.

## 🎓 Información Académica

* **Curso:** Big Data (202016911)

* **Actividad:** Tarea 3 - Procesamiento de Datos con Apache Spark

* **Institución:** Universidad Nacional Abierta y a Distancia (UNAD)

* **Escuela:** Escuela de Ciencias Básicas, Tecnología e Ingeniería (ECBTI)