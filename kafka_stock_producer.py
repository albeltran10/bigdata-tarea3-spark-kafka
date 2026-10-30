import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuración de activos y rangos base de precios
STOCKS = {
    "AAPL": 180.0,
    "MSFT": 420.0,
    "GOOGL": 175.0,
    "AMZN": 185.0,
    "TSLA": 250.0
}

def generate_stock_tick():
    """Genera una transacción de mercado simulada."""
    ticker = random.choice(list(STOCKS.keys()))
    base_price = STOCKS[ticker]
    # Simula una variación aleatoria entre -1.5% y +1.5%
    variation = random.uniform(-0.015, 0.015)
    current_price = round(base_price * (1 + variation), 2)
    volume = random.randint(10, 500)
    
    # Formato de fecha ISO compatible directo con TimestampType de Spark
    current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    return {
        "ticker": ticker,
        "price": current_price,
        "volume": volume,
        "timestamp": current_time
    }

# Inicializar productor Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

topic_name = "stock_ticks"
print(f">>> [PRODUCER] Enviando eventos en tiempo real al topic '{topic_name}' (Ctrl+C para detener)...")

try:
    while True:
        tick = generate_stock_tick()
        producer.send(topic_name, value=tick)
        print(f"Sent: {tick}")
        # Emite un evento cada 0.5 a 1 segundo para emular alta concurrencia
        time.sleep(random.uniform(0.5, 1.0))
except KeyboardInterrupt:
    print("\n>>> [PRODUCER] Transmisión detenida por el usuario.")
finally:
    producer.flush()
    producer.close()
