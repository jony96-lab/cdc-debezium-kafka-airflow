from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import dag  
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.smtp.operators.smtp import EmailOperator  
from datetime import datetime, timedelta
import logging


# --- CONFIGURACIÓN GENERAL DEL DAG ---
default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


# --- FUNCIÓN DE CHECKEO ---
def check_connector_status(response):
    import json
    try:
        j = response.json()
        connector_state = j.get('connector', {}).get('state')
        tasks = j.get('tasks', [])
        task_state = tasks[0].get('state') if tasks else None

        logging.info(f"STATUS CHECK: Connector={connector_state}, Task={task_state}")

        if connector_state == 'RUNNING' and task_state == 'RUNNING':
            return True

        logging.error(f"STATUS CHECK FAILED: Connector or task not in RUNNING state. Full response: {response.text}")
        return False
    except Exception as e:
        logging.error(f"Error parsing connector status response: {e}")
        return False


# ----------------------------------------------------
# DEFINICIÓN DEL DAG
# ----------------------------------------------------
@dag(
    dag_id="cdc_connector_health_monitor",  # ✅ Tu ID original
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["cdc", "monitoreo", "http", "gmail"],
    default_args=default_args,
)
def cdc_connector_health_monitor_dag():

    # --- Tarea 1: Verificar Debezium Source ---
    check_debezium_source = HttpOperator(
        task_id="check_mysql_connector_status",
        http_conn_id="kafka_connect_api",
        endpoint="/connectors/mysql-connector/status",  # fuerza error para probar alertas -FAIL
        method="GET",
        response_check=check_connector_status,
        log_response=True,
    )

    # --- Tarea 2: Verificar Postgres Sink ---
    check_postgres_sink = HttpOperator(
        task_id="check_postgres_sink_status",
        http_conn_id="kafka_connect_api",
        endpoint="/connectors/postgres-sink-connector/status",
        method="GET",
        response_check=check_connector_status,
        log_response=True,
    )

    # --- Tarea 3: Enviar correo de alerta (Gmail SMTP) ---
    send_failure_email_alert = EmailOperator(
        task_id="send_failure_email_alert_gmail",
        to=["@gmail.com"],
        subject="🚨 ALERTA AIRFLOW: Falla en Conector CDC ({{ ds }})",
        html_content="""
            <h3>Falla Crítica en el Monitoreo de Conectores CDC</h3>
            <p>Una de las tareas de chequeo (Source o Sink) ha fallado.</p>
            <p><strong>DAG:</strong> {{ dag.dag_id }}</p>
            <p><strong>Log URL:</strong> <a href="{{ ti.log_url }}">Ver Logs de Tarea</a></p>
            <br>
            <small>Este correo fue enviado automáticamente por Airflow vía Gmail SMTP.</small>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,  # se ejecuta solo si una falla
    )

    # --- Dependencias ---
    [check_debezium_source, check_postgres_sink] >> send_failure_email_alert


# Instancia del DAG
cdc_monitor_dag = cdc_connector_health_monitor_dag()
