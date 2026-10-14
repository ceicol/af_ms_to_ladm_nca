SET search_path TO 	
	input_{{ dag_run.conf.get('process_id', params.process_id) }}, -- Schema modelo simplificado
	output_{{ dag_run.conf.get('process_id', params.process_id) }}, -- schema modelo LADM extendido
	public;


insert into nca_fuenteadministrativa (
    t_basket,
    t_ili_tid,
    tipo,
    estado_disponibilidad,
    fecha_documento_fuente,
    nombre,
    url,
    espacio_de_nombres,
    local_id,
    etl_nacimientoagua_id
)
select
    (select t_id from output_{{ dag_run.conf.get('process_id', params.process_id) }}.t_ili2db_basket where topic = 'LADM_COL_v_1_0_0_Ext_NCA.NCA' limit 1) as t_basket,
    uuid_generate_v4() as t_ili_tid,
    tipo_fuente_administrativa as tipo,
    'Disponible' as estado_disponibilidad,
    fecha_fuente_administrativa as fecha_documento_fuente,
    nombre_fuente_administrativa as nombre,
    url_fuente_administrativa as url,
    'nca_fuenteadministrativa' as espacio_de_nombres,
    row_number() over (order by t_id)::text as local_id,
    t_id as etl_nacimientoagua_id
from ms_nca_nacimientoagua;