SET search_path TO 	
	input_{{ dag_run.conf.get('process_id', params.process_id) }}, -- Schema modelo simplificado
	output_{{ dag_run.conf.get('process_id', params.process_id) }}, -- schema modelo LADM extendido
	public;


alter table nca_fuenteadministrativa add column etl_nacimientoagua_id bigint;
alter table nca_uab_nacimientoagua add column etl_nacimientoagua_id bigint;
alter table nca_ue_nacimientoagua add column etl_nacimientoagua_id bigint;

-- Se crea el basket por defecto para el modelo LADM_COL
INSERT INTO output_{{ dag_run.conf.get('process_id', params.process_id) }}.t_ili2db_dataset (t_id,datasetname)
VALUES (1, 'Baseset');

INSERT INTO output_{{ dag_run.conf.get('process_id', params.process_id) }}.t_ili2db_basket (t_id,dataset,topic,t_ili_tid,attachmentkey,domains)
VALUES (1, 1, 'LADM_COL_v_1_0_0_Ext_NCA.NCA', uuid_generate_v4(), 'MADS', NULL);