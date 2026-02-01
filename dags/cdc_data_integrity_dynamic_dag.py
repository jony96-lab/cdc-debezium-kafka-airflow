from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging

# Constantes de configuración
MYSQL_CONN_ID = "mysql_cdc_source"
POSTGRES_CONN_ID = "postgres_cdc_sink"
SMTP_CONN_ID = "smtp_default" # Debes crear esta conexión en Airflow UI

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="cdc_data_integrity_check_pro",
    start_date=datetime(2025, 1, 1),
    schedule="0 8 * * *", # Ejecutar diariamente a las 8 AM
    catchup=False,
    tags=["calidad", "cdc", "critico"],
    default_args=default_args,
)
def integrity_check_dag():

    @task
    def get_monitored_tables():
        """Obtiene la lista de tablas desde una Variable de Airflow (JSON Array)."""
        # En Airflow UI -> Admin -> Variables: crear 'CDC_MONITORED_TABLES' con valor: ["ventas", "productos", "clientes"]
        tables = Variable.get("CDC_MONITORED_TABLES", deserialize_json=True, default_var=["ventas"])
        return tables

    @task
    def check_table_integrity(table_name: str):
        """
        Compara el MAX(ID) entre origen y destino. 
        Más eficiente que COUNT(*) para tablas grandes.
        """
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        logging.info(f"--- Iniciando chequeo para tabla: {table_name} ---")

        # 1. Obtener Max ID en MySQL
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        # Asumimos que todas tus tablas tienen una PK llamada 'id'. Si no, configurar un mapa de PKs.
        sql = f"SELECT MAX(id) FROM {table_name}"
        source_max = mysql_hook.get_first(sql)[0] or 0

        # 2. Obtener Max ID en Postgres
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        dest_max = pg_hook.get_first(sql)[0] or 0

        logging.info(f"Source Max ID: {source_max} | Dest Max ID: {dest_max}")

        if source_max != dest_max:
            raise ValueError(f"❌ DESFASE DETECTADO en {table_name}: Source({source_max}) != Sink({dest_max})")
        
        logging.info(f"✅ Integridad OK para {table_name}")

    # Notificación de error
    notify_error = EmailOperator(
        task_id="notify_integrity_error",
        to="jmaccari96@gmail.com",
        conn_id=SMTP_CONN_ID,
        subject="🚨 ALERTA DE INTEGRIDAD: Falla en replicación CDC",
        html_content="""
            <h3>Falla de Integridad de Datos</h3>
            <p>Se ha detectado una discrepancia de datos entre MySQL y Postgres.</p>
            <p>Revise los logs del DAG: <b>{{ dag.dag_id }}</b></p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # Flujo Dinámico
    tables = get_monitored_tables()
    checks = check_table_integrity.expand(table_name=tables)
    checks >> notify_error

dag_instance = integrity_check_dag()