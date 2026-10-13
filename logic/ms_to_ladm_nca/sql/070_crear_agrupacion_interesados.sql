SET search_path TO 	
	input_{{ dag_run.conf.get('process_id', params.process_id) }}, -- Schema modelo simplificado
	output_{{ dag_run.conf.get('process_id', params.process_id) }}, -- schema modelo LADM extendido
	public;


with nueva_agrupacion as (
    insert into nca_agrupacioninteresados (
        t_basket,
        t_ili_tid,
        tipo,
        nombre,
        tipo_interesado,
        comienzo_vida_util_version,
        espacio_de_nombres,
        local_id
    )
    select
        (select t_id from output_{{ dag_run.conf.get('process_id', params.process_id) }}.t_ili2db_basket tidb where topic  = 'LADM_COL_v_1_0_0_Ext_NCA.NCA' limit 1) as t_basket,
        uuid_generate_v4() as t_ili_tid,
        'Grupo_Empresarial' as tipo,
        'Comisión conjunta' as nombre,
        'Persona_Juridica' as tipo_interesado,
        now() as comienzo_vida_util_version,
        'nca_agrupacioninteresados' as espacio_de_nombres,
        '1' as local_id
    where (select count(*) from nca_interesado) > 1
    returning t_id, t_basket
)
insert into col_miembros (
    t_basket,
    interesado_nca_interesado,
    agrupacion
)
select
    g.t_basket as t_basket,
    i.t_id as interesado_nca_interesado,
    g.t_id as agrupacion
from nueva_agrupacion g
cross join nca_interesado i;