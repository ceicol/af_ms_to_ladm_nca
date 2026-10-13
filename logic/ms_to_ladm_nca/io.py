import os
from config.general_config import OUTPUT_DIR, S3_ROOT_STORAGE
from config.etl_config.ms_to_ladm_nca.config import ETL_NAME, DAG_NAME
from utils.file_utils import upload_file_to_aws_s3, compress_xtf_file
from airflow.providers.standard.operators.python import PythonOperator


def upload_to_s3_xtf_ladm(process_id: str):
    xtf_path = os.path.join(OUTPUT_DIR, ETL_NAME, process_id, 'output_{}.xtf'.format(process_id))
    xtf_zip_path = compress_xtf_file(xtf_path)
    s3_path = upload_file_to_aws_s3(
        file_path=xtf_zip_path,
        folder_name=os.path.join(S3_ROOT_STORAGE, DAG_NAME)
    )
    print(s3_path)

def task_publish_s3_xtf_ladm(process_id:str):
    return PythonOperator(
        task_id='publicar_en_s3_datos_zip_xtf_ladm_col',
        python_callable=upload_to_s3_xtf_ladm,
        op_kwargs={"process_id": process_id},
    )
