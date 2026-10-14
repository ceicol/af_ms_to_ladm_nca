from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

from utils.file_utils import download_or_copy_xtf
from config.etl_config.ms_to_ladm_nca.config import ETL_NAME, ETL_TASKS




def create_sql_task(
    postgres_conn_id: str, task_id: str, sql_file: str
) -> SQLExecuteQueryOperator:
    """Construye una tarea SQL con la conexión PostgreSQL indicada."""
    return SQLExecuteQueryOperator(
        task_id=task_id,
        conn_id=postgres_conn_id,
        sql=sql_file,
    )


def create_etl_tasks(postgres_conn_id: str) -> dict[str, SQLExecuteQueryOperator]:
    """Construye las tareas SQL en el contexto del DAG activo, indexadas por task_id."""
    return {
        task_id: create_sql_task(postgres_conn_id, task_id, sql_file)
        for task_id, sql_file in ETL_TASKS
    }


# =========================
# FILE UTIL
# =========================

def task_download_xtf(process_id:str, url_xtf:str):
    return PythonOperator(
        task_id='download_xtf',
        python_callable=download_or_copy_xtf,
        op_kwargs={"process_id": process_id, "source": url_xtf, "etl_name": ETL_NAME},
    )
