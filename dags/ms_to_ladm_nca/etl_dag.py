import os
import uuid
from datetime import datetime

from airflow import DAG
from airflow.sdk import Param
from airflow.providers.standard.operators.empty import EmptyOperator

from config.etl_config.ms_to_ladm_nca.config import (
    DAG_NAME,
    DAG_DESCRIPTION,
    ETL_TAGS
)

from logic.ms_to_ladm_nca.models import (
    task_import_schema_ms,
    task_import_schema_ladm,
    task_import_data_ms,
    task_export_xtf_ladm,
)

from logic.ms_to_ladm_nca.workflow import (
    create_etl_tasks,
    task_download_xtf,
)

from utils.tasks_utils import (
    task_validar_conexion,
)

from logic.ms_to_ladm_nca.io import task_publish_s3_xtf_ladm

# Leer el nombre de la conexión desde la variable de entorno
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "terrainfo_etl")
PROCESS_ID = "{{ dag_run.conf.get('process_id', params.process_id) }}"
REMOTE_XTF_PATH = "{{ dag_run.conf.get('xtf_path', params.xtf_path) }}"

with DAG(
    dag_id=DAG_NAME,
    description=DAG_DESCRIPTION,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Solo ejecución manual
    render_template_as_native_obj=True,      # respeta tipos al renderizar
    catchup=False,
    params={
        "process_id": Param(
            default=str(uuid.uuid4().hex),
            type="string",
            description="Identificador único del proceso"
        ),
        "xtf_path": Param(
            default='https://terrainfo.nyc3.cdn.digitaloceanspaces.com/OTLS/ms_nca.xtf',
            type="string",
            description="URL del XTF a procesar"
        ),
    },
    tags=ETL_TAGS,
    template_searchpath="/opt/airflow/logic"  # Ruta en la cual es buscada los archivos SQL al aplicar jinja
) as dag:

    etl_tasks_ms_to_ladm = create_etl_tasks(POSTGRES_CONN_ID)

    # Ejecución del ETL
    (
        EmptyOperator(task_id="inicio") >>
        task_validar_conexion(postgres_conn_id=POSTGRES_CONN_ID) >>
        task_download_xtf(PROCESS_ID, REMOTE_XTF_PATH) >>
        task_import_schema_ms(PROCESS_ID) >>
        task_import_data_ms(PROCESS_ID, REMOTE_XTF_PATH) >>
        task_import_schema_ladm(PROCESS_ID) >>
        
        # Inicia transformación de datos al modelo LADM_COL
        # Nota: El orden importa; debe corresponder al flujo de ejecución
        # del ETL para asegurar la integridad referencial de los datos.
        etl_tasks_ms_to_ladm["preparar_estructura_etl"] >>
        etl_tasks_ms_to_ladm["migrar_interesados"] >>
        etl_tasks_ms_to_ladm["migrar_fuentes_administrativas"] >>
        etl_tasks_ms_to_ladm["migrar_unidad_administrativa"] >>
        etl_tasks_ms_to_ladm["migrar_unidad_espacial"] >>
        etl_tasks_ms_to_ladm["diligenciar_relacion_entre_unidad_administrativa_y_espacial"] >>
        etl_tasks_ms_to_ladm["crear_estructura_col_area_valor"] >>
        etl_tasks_ms_to_ladm["crear_agrupacion_interesados"] >>
        etl_tasks_ms_to_ladm["crear_responsabilidades"] >>
        etl_tasks_ms_to_ladm["diligenciar_relacion_col_rrrfuente"] >>

        task_export_xtf_ladm(PROCESS_ID) >>
        task_publish_s3_xtf_ladm(PROCESS_ID) >>
        EmptyOperator(task_id="fin")
    )
