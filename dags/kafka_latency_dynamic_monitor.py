from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG 
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)

# --- 1. CONFIGURACIÓN DEL SISTEMA POS ---
# Lista real de tus tablas
EXPECTED_TABLES = [
    "ventas", 
    "detalle_ventas", 
    "productos", 
    "entidades", 
    "compras", 
    "detalle_compras",
    "cierres_caja",
    "pagos_clientes"
]
TOPIC_PREFIX = "mysql-server.pos_bi_db" # Prefijo correcto para tu DB

# --- 2. Parámetros de Kafka ---
KAFKA_BROKER = "kafka:29092"
SINK_CONSUMER_GROUP = "connect-postgres-sink-connector"
MAX_LAG_THRESHOLD = 500

def notify_on_failure(context):
    task_instance = context.get('ti')
    logging.error(f"⚠️ ALERTA: Kafka Lag falló en {task_instance.task_id}")

def check_kafka_lag_status(**kwargs):
    try:
        from kafka import KafkaConsumer, TopicPartition
    except ImportError:
        raise Exception("Falta librería kafka-python.")

    logging.info(f"Iniciando chequeo de Kafka Lag en {KAFKA_BROKER}")
    
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        group_id=SINK_CONSUMER_GROUP,
        enable_auto_commit=False,
        consumer_timeout_ms=5000
    )

    has_critical_lag = False
    
    for table in EXPECTED_TABLES:
        source_topic = f"{TOPIC_PREFIX}.{table}"
        tp = TopicPartition(source_topic, 0)

        try:
            end_offsets = consumer.end_offsets([tp])
            latest_offset = end_offsets.get(tp) if end_offsets else 0
            
            committed_offset = consumer.committed(tp)
            current_offset = committed_offset if committed_offset is not None else 0
            
            lag = latest_offset - current_offset
            if latest_offset == 0: lag = 0

            logging.info(f"METRICS ({table}): Latest={latest_offset}, Current={current_offset}, Lag={lag}")

            if lag > MAX_LAG_THRESHOLD:
                logging.error(f"❌ LAG CRÍTICO ({table}): {lag} > {MAX_LAG_THRESHOLD}")
                has_critical_lag = True

        except Exception as e:
            logging.warning(f"No se pudo leer métricas para {source_topic} (¿Quizás tabla vacía?): {e}")

    consumer.close()

    if has_critical_lag:
        raise Exception("Retraso crítico detectado en replicación.")
    else:
        logging.info("✅ Kafka Lag OK.")
        return True

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_on_failure, 
}

with DAG(
    dag_id="kafka_latency_monitor_DYNAMIC",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *", 
    catchup=False,
    tags=["cdc", "rendimiento", "kafka", "lag"],
    default_args=default_args,
) as latency_monitor_dag:

    check_lag = PythonOperator(
        task_id="check_consumer_lag_all_topics",
        python_callable=check_kafka_lag_status,
    )