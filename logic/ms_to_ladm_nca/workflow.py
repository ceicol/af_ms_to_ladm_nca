import os

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

from utils.file_utils import download_or_copy_xtf
from config.etl_config.ms_to_ladm_nca.config import ETL_NAME

# =========================
# ETL WORKFLOW
# =========================

def task_preparar_estructura_etl(postgres_conn_id: str):
    return SQLExecuteQueryOperator(
        task_id="preparar_estructura_etl",
        conn_id=postgres_conn_id,
        sql="ms_to_ladm_nca/sql/000_preparar_estructura_etl.sql",
    )

def task_migrar_interesados(postgres_conn_id: str):
    return SQLExecuteQueryOperator(
        task_id="migrar_interesados",
        conn_id=postgres_conn_id,
        sql="ms_to_ladm_nca/sql/010_migrar_interesados.sql",
    )

def task_migrar_fuentes_administrativas(postgres_conn_id: str):
    return SQLExecuteQueryOperator(
        task_id="migrar_fuentes_administrativas",
        conn_id=postgres_conn_id,
        sql="ms_to_ladm_nca/sql/020_migrar_fuentes_administrativas.sql",
    )

def task_migrar_unidad_administrativa(postgres_conn_id: str):
    return SQLExecuteQueryOperator(
        task_id="migrar_unidad_administrativa",
        conn_id=postgres_conn_id,
        sql="ms_to_ladm_nca/sql/030_migrar_unidad_administrativa.sql",
    )

def task_migrar_unidad_espacial(postgres_conn_id: str):
    return SQLExecuteQueryOperator(
        task_id="migrar_unidad_espacial",
        conn_id=postgres_conn_id,
        sql="ms_to_ladm_nca/sql/040_migrar_unidad_espacial.sql",
    )

def task_diligenciar_relacion_entre_unidad_administrativa_y_espacial(postgres_conn_id: str):
    return SQLExecuteQueryOperator(
        task_id="diligenciar_relacion_entre_unidad_administrativa_y_espacial",
        conn_id=postgres_conn_id,
        sql="ms_to_ladm_nca/sql/050_diligenciar_relacion_entre_unidad_administrativa_y_espacial.sql",
    )

def task_crear_estructura_col_area_valor(postgres_conn_id: str):
    return SQLExecuteQueryOperator(
        task_id="crear_estructura_col_area_valor",
        conn_id=postgres_conn_id,
        sql="ms_to_ladm_nca/sql/060_crear_estructura_col_area_valor.sql",
    )

def task_crear_agrupacion_interesados(postgres_conn_id: str):
    return SQLExecuteQueryOperator(
        task_id="crear_agrupacion_interesados",
        conn_id=postgres_conn_id,
        sql="ms_to_ladm_nca/sql/070_crear_agrupacion_interesados.sql",
    )

def task_crear_responsabilidades(postgres_conn_id: str):
    return SQLExecuteQueryOperator(
        task_id="crear_responsabilidades",
        conn_id=postgres_conn_id,
        sql="ms_to_ladm_nca/sql/080_crear_responsabilidades.sql",
    )

def task_diligenciar_relacion_col_rrrfuente(postgres_conn_id: str):
    return SQLExecuteQueryOperator(
        task_id="diligenciar_relacion_col_rrrfuente",
        conn_id=postgres_conn_id,
        sql="ms_to_ladm_nca/sql/090_diligenciar_relacion_col_rrrfuente.sql",
    )

# =========================
# FILE UTIL
# =========================

def task_download_xtf(process_id:str, url_xtf:str):
    return PythonOperator(
        task_id='download_xtf',
        python_callable=download_or_copy_xtf,
        op_kwargs={"process_id": process_id, "source": url_xtf, "etl_name": ETL_NAME},
    )
