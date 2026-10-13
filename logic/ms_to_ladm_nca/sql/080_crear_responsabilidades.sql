SET search_path TO 	
	input_{{ dag_run.conf.get('process_id', params.process_id) }}, -- Schema modelo simplificado
	output_{{ dag_run.conf.get('process_id', params.process_id) }}, -- schema modelo LADM extendido
	public;


with interesado_responsable as (
    select
        i.t_id as interesado,
        null::bigint as agrupacion
    from nca_interesado i
    where (select count(*) from nca_interesado) = 1
    union all
    select
        null::bigint,
        cm.agrupacion
    from col_miembros cm
    group by cm.agrupacion
    having count(distinct cm.interesado_nca_interesado) > 1
)
insert into nca_responsabilidad (
    t_basket,
    tipo,
    descripcion,
    interesado_nca_interesado,
    interesado_nca_agrupacioninteresados,
    unidad,
    comienzo_vida_util_version,
    espacio_de_nombres,
    local_id
)
select
    (select t_id from output_{{ dag_run.conf.get('process_id', params.process_id) }}.t_ili2db_basket tidb where topic  = 'LADM_COL_v_1_0_0_Ext_NCA.NCA' limit 1) as t_basket,
    'Identificar' as tipo,
    'Identificacion del nacimiento de agua ' || uab.nombre_nacimiento as descripcion,
    interesado as interesado_nca_interesado,
    agrupacion as interesado_nca_agrupacioninteresados,
    uab.t_id as unidad,
    now() as comienzo_vida_util_version,
    'nca_responsabilidad' as espacio_de_nombres,
    row_number() over (order by uab.t_id)::text as local_id,
from nca_uab_nacimientoagua uab
cross join interesado_responsable;