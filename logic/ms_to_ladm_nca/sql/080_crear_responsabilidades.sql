SET search_path TO
    input_{{ dag_run.conf.get('process_id', params.process_id) }}, -- Schema modelo simplificado
    output_{{ dag_run.conf.get('process_id', params.process_id) }}, -- schema modelo LADM extendido
    public;

--========================================================================
-- NCA_Responsabilidad: la autoridad ambiental identifica el nacimiento de agua
-- Una responsabilidad por nacimiento: se vincula al unico interesado o a la
-- agrupacion de sus miembros. Sin interesados no se crean responsabilidades.
-- col_rrrInteresado esta integrada en estas dos columnas del DRR por ili2pg.
--========================================================================
with interesado_responsable as (
    select
        t_id as interesado,
        null::bigint as agrupacion
    from nca_interesado
    where (select count(*) from nca_interesado) = 1
    union all
    select
        null::bigint,
        agrupacion
    from col_miembros
    group by agrupacion
    having count(distinct interesado_nca_interesado) > 1
)
insert into nca_responsabilidad (
    t_basket,
    t_ili_tid,
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
    (select t_id from output_{{ dag_run.conf.get('process_id', params.process_id) }}.t_ili2db_basket where topic = 'LADM_COL_v_1_0_0_Ext_NCA.NCA' limit 1) as t_basket,
    uuid_generate_v4() as t_ili_tid,
    'Identificar' as tipo,
    'Identificacion del nacimiento de agua ' || uab.nombre_nacimiento as descripcion,
    interesado as interesado_nca_interesado,
    agrupacion as interesado_nca_agrupacioninteresados,
    uab.t_id as unidad,
    now() as comienzo_vida_util_version,
    'nca_responsabilidad' as espacio_de_nombres,
    row_number() over (order by uab.t_id)::text as local_id
from nca_uab_nacimientoagua uab
cross join interesado_responsable;
