## 🏗️ Arquitectura del Proyecto
<p align="center">
  <img src="img/arquitectura.png" width="800" title="Arquitectura CDC">
</p>

## 📈 Monitoreo y Observabilidad
<p align="center">
  <img src="img/airflow-dags.png" width="800" title="Airflow Dashboard">
</p>



Real-Time CDC Pipeline
Este proyecto implementa una arquitectura de Change Data Capture (CDC) de nivel industrial para replicar datos en tiempo real desde un sistema transaccional (MySQL) hacia un Data Warehouse (Postgres), garantizando la integridad y salud del pipeline mediante Apache Airflow 3.1.1.

🚀 Vista General de la Solución
El sistema resuelve el problema de la latencia en la toma de decisiones, transformando un Punto de Venta (POS) tradicional en una plataforma de datos orientada a eventos.

Ingesta: Captura de cambios basada en logs (Log-based CDC) con Debezium.

Mensajería: Streaming de eventos distribuido con Kafka (Modo KRaft).

Almacenamiento: Sincronización automática con JDBC Sink Connector hacia Postgres.

Orquestación y Observabilidad: Monitoreo proactivo con Airflow 3, utilizando Dynamic Task Mapping para escalar el monitoreo de tablas e integridad.

🛠️ Stack Tecnológico
Core: Docker & Docker Compose.

Data Streaming: Kafka 7.6 (Confluent), Debezium 2.5.

Orquestador: Apache Airflow 3.1.1 (Latest).

Bases de Datos: MySQL (Origen) y PostgreSQL (Destino).

📊 Arquitectura de Monitoreo (Airflow)
La pieza clave es la capa de observabilidad. He desarrollado DAGs dinámicos que aseguran la confiabilidad del dato:

Connector Health Monitor: Chequea la API de Kafka Connect cada 5 minutos. Si un conector pasa a estado FAILED, se dispara una alerta inmediata.

Data Integrity Check: Realiza validaciones cruzadas (COUNT(*)) entre origen y destino para detectar desfasajes en la replicación.

Alerting System: Integración con SMTP (Gmail) para notificaciones críticas de fallos en el pipeline.

⚙️ Configuración del Pipeline
1. Conector de Origen (Debezium MySql)
Configurado para capturar todos los cambios del esquema pos_bi_db. El uso de table.include.list permite un escalado controlado de las entidades a replicar.

JSON
{
  "name": "mysql-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.include.list": "pos_bi_db",
    "topic.prefix": "mysql-server",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:29092"
  }
}
2. Conector de Destino (JDBC Sink)
Implementa Upsert logic y Automatic Schema Evolution, lo que permite que el Data Warehouse se adapte a cambios menores en el origen sin intervención manual.

📝 Guía de Operación (Escalabilidad)
Para agregar una nueva tabla al monitoreo e integridad:

Postgres: Crear la tabla espejo en el destino.

Connectors: Actualizar la lista de tablas en los archivos JSON de configuración.

Airflow UI: Simplemente actualizar la variable CDC_MONITORED_TABLES desde la interfaz de Airflow. No requiere reinicio de servicios.