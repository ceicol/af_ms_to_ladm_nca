{% set process_id = dag_run.conf.get('process_id', params.process_id) %}
SET search_path TO
    input_{{ process_id }}, -- Schema modelo simplificado
    output_{{ process_id }}, -- schema modelo LADM extendido
    public;

--========================================================================
-- COL_AreaValor: area grafica en m2 de cada unidad espacial con geometria
--========================================================================
insert into col_areavalor (
    t_basket,
    tipo,
    area,
    nca_ue_nacimientoagua_area
)
select
    (select t_id from output_{{ process_id }}.t_ili2db_basket where topic = 'LADM_COL_v_1_0_0_Ext_NCA.NCA' limit 1) as t_basket,
    'Area_Grafica' as tipo,
    st_area(st_makevalid(st_force2d(geometria)))::numeric as area,
    t_id as nca_ue_nacimientoagua_area
from nca_ue_nacimientoagua where geometria is not null;
