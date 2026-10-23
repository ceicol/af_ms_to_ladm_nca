{% set process_id = dag_run.conf.get('process_id', params.process_id) %}
SET search_path TO
    input_{{ process_id }}, -- Schema modelo simplificado
    output_{{ process_id }}, -- schema modelo LADM extendido
    public;

--========================================================================
-- COL_RRRFuente: fuente administrativa de cada responsabilidad
--   La unidad de la responsabilidad identifica el nacimiento de origen.
--========================================================================
insert into col_rrrfuente (
    t_basket,
    fuente_administrativa,
    rrr_nca_responsabilidad
)
select distinct
    (select t_id from output_{{ process_id }}.t_ili2db_basket where topic = 'LADM_COL_v_1_0_0_Ext_NCA.NCA' limit 1) as t_basket,
    f.t_id as fuente_administrativa,
    r.t_id as rrr_nca_responsabilidad
from nca_responsabilidad r
join nca_uab_nacimientoagua uab on uab.t_id = r.unidad
join nca_fuenteadministrativa f on f.etl_nacimientoagua_id = uab.etl_nacimientoagua_id;
