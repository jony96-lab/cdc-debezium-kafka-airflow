from airflow.providers.standard.operators.python import PythonOperator
from airflow.models.dag import DAG 
from datetime import datetime, timedelta
import logging

# Configuración de Logging
logging.basicConfig(level=logging.INFO)

# -------------------------------------------------------------------
# !!! AJUSTA ESTOS VALORES A TU ENTORNO KAFKA !!!
# -------------------------------------------------------------------
KAFKA_BROKER = "kafka:29092"                    # Host:Puerto de tu broker de Kafka
SINK_CONSUMER_GROUP = "connect-postgres-sink-connector" # Nombre del Consumer Group de tu Sink Connector
SOURCE_TOPIC = "mysql-server.pos_bi_db.customers" # Nombre del Tópico que replica tu tabla 'customers'
MAX_LAG_THRESHOLD = 500                         # Umbral Máximo de Lag Aceptable (ej: 500 mensajes)
# -------------------------------------------------------------------

# --- FUNCIÓN DE ALERTA DE FALLO ---
def notify_on_failure(context):
    """ Función para alertar sobre la falla del DAG de Lag. """
    task_instance = context.get('ti')
    logging.error(f"================================================================")
    logging.error(f"⚠️ ALERTA DE RENDIMIENTO: Kafka Lag falló en Tarea: {task_instance.task_id}")
    logging.error(f"================================================================")

# --- FUNCIÓN CENTRAL DE CHEQUEO DE LAG ---
# --- FUNCIÓN CENTRAL DE CHEQUEO DE LAG (Corregida) ---
def check_kafka_lag_status(**kwargs):
    """
    Se conecta al broker de Kafka y usa KafkaConsumer para obtener el lag.
    """
    try:
        # Usamos KafkaConsumer y TopicPartition
        from kafka import KafkaConsumer, TopicPartition
    except ImportError:
        logging.error("❌ Falla crítica: La librería 'kafka-python' no está instalada.")
        raise Exception("Falta dependencia 'kafka-python' para monitorear el Lag.")

    # Parámetros (Asegúrate de que sean correctos)
    KAFKA_BROKER = "kafka:29092"                    
    SINK_CONSUMER_GROUP = "connect-postgres-sink-connector" 
    SOURCE_TOPIC = "mysql-server.pos_bi_db.customers" 
    MAX_LAG_THRESHOLD = 500                         # Tu umbral
    
    logging.info(f"Iniciando chequeo de Kafka Lag en {KAFKA_BROKER}")

    try:
        # Definir el tópico y la partición a revisar (asumiendo partición 0)
        tp = TopicPartition(SOURCE_TOPIC, 0)

        # 1. Crear un consumidor temporal para obtener los offsets
        consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BROKER],
            group_id=SINK_CONSUMER_GROUP,
            enable_auto_commit=False, # Importante: no queremos que haga commit
            consumer_timeout_ms=5000 # Timeout para la operación
        )
        
        # 2. Obtener el OFFSET FINAL (El último mensaje escrito por Debezium)
        end_offsets = consumer.end_offsets([tp])
        latest_offset = end_offsets.get(tp) if end_offsets else 0
        
        # 3. Obtener el OFFSET ACTUAL (El último mensaje leído por el Sink)
        # Esto nos da la posición actual del consumer group
        committed_offset = consumer.committed(tp)
        current_offset = committed_offset if committed_offset is not None else 0

        consumer.close()
        
        lag = latest_offset - current_offset
        
        if latest_offset == 0:
             # Puede pasar si el tópico está vacío.
            logging.info("Tópico vacío o no visible. Lag asumido como 0.")
            lag = 0

        logging.info(f"KAFKA METRICS: Último Offset (Producer): {latest_offset}")
        logging.info(f"KAFKA METRICS: Offset Consumido (Sink): {current_offset}")
        logging.info(f"KAFKA LAG para {SOURCE_TOPIC}: {lag} mensajes.")

        # 4. Comprobar el umbral
        if lag > MAX_LAG_THRESHOLD:
            logging.error(f"❌ ALERTA DE LAG: {lag} excede el umbral crítico ({MAX_LAG_THRESHOLD}).")
            raise Exception("Retraso crítico de Kafka detectado.")
        
        logging.info("✅ Kafka Lag OK: El Sink está consumiendo a tiempo.")
        return True

    except Exception as e:
        logging.error(f"Error al verificar Kafka Lag: {e}")
        # Re-lanzamos la excepción para marcar la tarea como fallida
        raise


# --- DEFINICIÓN DEL DAG ---
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_on_failure, 
}

with DAG(
    dag_id="kafka_latency_monitor",
    start_date=datetime(2025, 1, 1),
    # Se ejecuta cada 2 minutos para detectar cambios de rendimiento rápidamente
    schedule="*/2 * * * *", 
    catchup=False,
    tags=["cdc", "rendimiento", "kafka", "lag"],
    default_args=default_args,
) as latency_monitor_dag:

    check_lag = PythonOperator(
        task_id="check_consumer_lag",
        python_callable=check_kafka_lag_status,
        # No se usa provide_context=True en Airflow 2.x
    )