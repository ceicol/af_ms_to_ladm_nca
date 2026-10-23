{% set process_id = dag_run.conf.get('process_id', params.process_id) %}
SET search_path TO
    input_{{ process_id }}, -- Schema modelo simplificado
    output_{{ process_id }}, -- schema modelo LADM extendido
    public;

--========================================================================
-- NCA_UAB_NacimientoAgua  <-  MS_NCA_NacimientoAgua
--========================================================================
insert into nca_uab_nacimientoagua (
    t_basket,
    t_ili_tid,
    nombre_nacimiento,
    coordenada_x,
    coordenada_y,
    radio_proteccion,
    fecha_identificacion_nacimiento,
    observaciones,
    nombre,
    tipo,
    comienzo_vida_util_version,
    espacio_de_nombres,
    local_id,
    etl_nacimientoagua_id
)
select
    (select t_id from output_{{ process_id }}.t_ili2db_basket where topic = 'LADM_COL_v_1_0_0_Ext_NCA.NCA' limit 1) as t_basket,
    uuid_generate_v4() as t_ili_tid,
    nombre as nombre_nacimiento,
    coordenada_x as coordenada_x,
    coordenada_y as coordenada_y,
    radio_proteccion as radio_proteccion,
    fecha_identificacion as fecha_identificacion_nacimiento,
    observaciones as observaciones,
    nombre as nombre,
    'Ambiente_Desarrollo_Sostenible' as tipo,
    now() as comienzo_vida_util_version,
    'nca_uab_nacimientoagua' as espacio_de_nombres,
    row_number() over (order by t_id)::text as local_id,
    t_id as etl_nacimientoagua_id
from ms_nca_nacimientoagua;
