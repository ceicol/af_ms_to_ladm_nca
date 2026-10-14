SET search_path TO 	
	input_{{ dag_run.conf.get('process_id', params.process_id) }}, -- Schema modelo simplificado
	output_{{ dag_run.conf.get('process_id', params.process_id) }}, -- schema modelo LADM extendido
	public;


insert into col_rrrfuente (
    t_basket,
    fuente_administrativa,
    rrr_nca_responsabilidad
)
select distinct
    (select t_id from output_{{ dag_run.conf.get('process_id', params.process_id) }}.t_ili2db_basket where topic = 'LADM_COL_v_1_0_0_Ext_NCA.NCA' limit 1) as t_basket,
    f.t_id as fuente_administrativa,
    r.t_id as rrr_nca_responsabilidad
from nca_responsabilidad r
join nca_uab_nacimientoagua uab on uab.t_id = r.unidad
join nca_fuenteadministrativa f on f.etl_nacimientoagua_id = uab.etl_nacimientoagua_id;