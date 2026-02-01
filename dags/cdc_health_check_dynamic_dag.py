from airflow.decorators import dag, task
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

@dag(
    dag_id="cdc_connector_health_monitor_pro",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *", # Cada 5 minutos
    catchup=False,
    tags=["infra", "monitoreo", "kafka"],
    default_args=default_args,
)
def health_check_dag():

    # En Airflow UI -> Variables: crear 'CDC_CONNECTORS_LIST' con valor: ["mysql-connector", "postgres-sink-connector"]
    connectors = Variable.get("CDC_CONNECTORS_LIST", deserialize_json=True, default_var=["mysql-connector", "postgres-sink-connector"])

    def check_status_logic(response):
        import json
        try:
            data = response.json()
            state = data.get('connector', {}).get('state')
            # Validamos que el conector esté RUNNING
            if state != 'RUNNING':
                logging.error(f"Conector caído: {state}")
                return False
            
            # Validamos que TODAS las tareas estén RUNNING
            tasks = data.get('tasks', [])
            if not tasks:
                logging.warning("El conector no tiene tareas activas.")
                return False
                
            for t in tasks:
                if t.get('state') != 'RUNNING':
                    logging.error(f"Tarea fallida: {t}")
                    return False
            
            return True
        except Exception as e:
            logging.error(f"Error parseando respuesta: {e}")
            return False

    # Generación dinámica de tareas de chequeo
    check_tasks = HttpOperator.partial(
        task_id="check_connector",
        http_conn_id="kafka_connect_api", # Crear conexión HTTP en Admin: http://connect:8083
        method="GET",
        response_check=check_status_logic,
        log_response=True
    ).expand(
        endpoint=[f"/connectors/{name}/status" for name in connectors]
    )

    alert_email = EmailOperator(
        task_id="send_alert",
        to="jmaccari96@gmail.com",
        conn_id="smtp_default",
        subject="🚨 CRITICAL: Conector de Kafka Caído",
        html_content="""
            <h3>Alerta de Infraestructura</h3>
            <p>Uno o más conectores de Kafka Connect han reportado estado FAILED o UNASSIGNED.</p>
            <p>Verificar inmediatamente Kafka UI.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED
    )

    check_tasks >> alert_email

dag_instance = health_check_dag()