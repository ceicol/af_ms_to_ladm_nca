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
    task_download_xtf,
    task_preparar_estructura_etl,
    task_migrar_interesados,
    task_migrar_fuentes_administrativas,
    task_migrar_unidad_administrativa,
    task_migrar_unidad_espacial,
    task_diligenciar_relacion_entre_unidad_administrativa_y_espacial,
    task_crear_estructura_col_area_valor,
    task_crear_agrupacion_interesados,
    task_crear_responsabilidades,
    task_diligenciar_relacion_col_rrrfuente,
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


    # Ejecución del ETL
    (
        EmptyOperator(task_id="inicio") >>
        task_validar_conexion(postgres_conn_id=POSTGRES_CONN_ID) >>
        task_download_xtf(PROCESS_ID, REMOTE_XTF_PATH) >>
        task_import_schema_ms(PROCESS_ID) >>
        task_import_data_ms(PROCESS_ID, REMOTE_XTF_PATH) >>
        task_import_schema_ladm(PROCESS_ID) >>
        
        # Inicia transformación de datos al modelo LADM_COL
        task_preparar_estructura_etl(PROCESS_ID) >>
        task_migrar_interesados(PROCESS_ID) >>
        task_migrar_fuentes_administrativas(PROCESS_ID) >>
        task_migrar_unidad_administrativa(PROCESS_ID) >>
        task_migrar_unidad_espacial(PROCESS_ID) >>
        task_diligenciar_relacion_entre_unidad_administrativa_y_espacial(PROCESS_ID) >>
        task_crear_estructura_col_area_valor(PROCESS_ID) >>
        task_crear_agrupacion_interesados(PROCESS_ID) >>
        task_crear_responsabilidades(PROCESS_ID) >>
        task_diligenciar_relacion_col_rrrfuente(PROCESS_ID) >>
        # Finaliza transformación de datos al modelo LADM_COL

        task_export_xtf_ladm(PROCESS_ID) >>
        task_publish_s3_xtf_ladm(PROCESS_ID) >>
        EmptyOperator(task_id="fin")
    )
