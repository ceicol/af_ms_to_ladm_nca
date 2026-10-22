{% set process_id = dag_run.conf.get('process_id', params.process_id) %}
SET search_path TO
    input_{{ process_id }}, -- Schema modelo simplificado
    output_{{ process_id }}, -- schema modelo LADM extendido
    public;

--========================================================================
-- NCA_UE_NacimientoAgua  <-  geometria de MS_NCA_NacimientoAgua
--   El area se registra en COL_AreaValor, como define el modelo.
--========================================================================
insert into nca_ue_nacimientoagua (
    t_basket,
    t_ili_tid,
    etiqueta,
    relacion_superficie,
    geometria,
    comienzo_vida_util_version,
    espacio_de_nombres,
    local_id,
    etl_nacimientoagua_id
)
select
    (select t_id from output_{{ process_id }}.t_ili2db_basket where topic = 'LADM_COL_v_1_0_0_Ext_NCA.NCA' limit 1) as t_basket,
    uuid_generate_v4() as t_ili_tid,
    nombre as etiqueta,
    'En_Rasante' as relacion_superficie,
    st_force3d(geometria) as geometria,
    now() as comienzo_vida_util_version,
    'nca_ue_nacimientoagua' as espacio_de_nombres,
    row_number() over (order by t_id)::text as local_id,
    t_id as etl_nacimientoagua_id
from ms_nca_nacimientoagua;
