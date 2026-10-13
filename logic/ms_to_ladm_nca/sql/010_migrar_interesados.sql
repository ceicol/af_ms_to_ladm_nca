SET search_path TO 	
	input_{{ dag_run.conf.get('process_id', params.process_id) }}, -- Schema modelo simplificado
	output_{{ dag_run.conf.get('process_id', params.process_id) }}, -- schema modelo LADM extendido
	public;

insert into nca_interesado (
    t_basket,
    t_ili_tid,
    nombre,
    sigla,
    numero_documento,
    tipo_interesado,
    tipo_documento,
    comienzo_vida_util_version,
    espacio_de_nombres,
    local_id
)
select
    (select t_id from output_{{ dag_run.conf.get('process_id', params.process_id) }}.t_ili2db_basket tidb where topic  = 'LADM_COL_v_1_0_0_Ext_NCA.NCA' limit 1) as t_basket,
    uuid_generate_v4() as t_ili_tid,
    nombre as nombre,
    sigla as sigla,
    numero_documento as numero_documento,
    'Persona_Juridica' as tipo_interesado,
    'NIT' as tipo_documento,
    now() as comienzo_vida_util_version,
    'nca_interesado' as espacio_de_nombres,
    row_number() over (order by t_id)::text as local_id
from ms_nca_autoridadambiental;