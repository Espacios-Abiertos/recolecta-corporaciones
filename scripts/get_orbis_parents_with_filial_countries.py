# Creado: 2024-07-24
# Ya tenemos el network de corporaciones de Orbis descargado y listo para analizar (ver 'describe_orbis_corporate_network.py').
# Ahora vamos a hacer un resumen de las parent companies incluyendo: pais de la parent, paises de filiales

import os

import duckdb
import polars as pl
from utils_database import read_query

orbis_results_name = 'orbis_network_results_2024_07_23' # corresponde al directorio a abrir dentro de 'outputs/'
orbis_results_output_dir = f'outputs/{orbis_results_name}'

duckdb.sql('install spatial; load spatial;') # For Excel import
duckdb.sql(read_query('attach_db_readonly')) # Para leer info corporaciones

duckdb.sql(
'''
create view corporaciones_boricuas as (
    select distinct registration_index, corp_name, status_id, status_es
    from recolecta_corporaciones.recolecta_buscador__records
    order by record_id
);
'''
)

duckdb.sql(
'''
create view orbis_data_results as (
    select * replace (operating_revenue_year_2024_eur_th::double as operating_revenue_year_2024_eur_th)
    from read_parquet('outputs/orbis_search_results_2024_07_21/orbis_data_results.parquet')
)
'''
)

duckdb.sql(
f'''
create view orbis_network_results as (
    from read_parquet('{orbis_results_output_dir}/orbis_network_results.parquet')
)
'''
)

duckdb.sql(
'''
create table orbis_parent_companies as (
    select
        "BvD ID number" as bvd_id_orbis,
        "Company name Latin alphabet" as company_name,
        "Country" as country,
    from st_read('outputs/orbis_search_results_2024_07_21/Parent Companies Final 07.22 (version 1).xlsx',
        layer = 'Results', open_options= ['HEADERS=FORCE'])
)
'''
)

duckdb.sql(
'''
create view qualified_companies_pr_orbis_search as (
    from read_csv_auto('outputs/orbis_search_results_2024_07_21/qualified_company_number_pr.csv')
)
'''
)

duckdb.sql('''
create view countries_iso_data as (
    from read_csv_auto('inputs/ISO.csv')
)           
''')

duckdb.sql(
'''
create table pillar2_pwc_report as (
    select 
    "alpha-2" as country_code,
    pwc.country, "year"::INT as "year", pwc.pillar2, iir, utpr, qdmtt
    from st_read('inputs/pillar2_PWC.xlsx',
            layer = 'pillar2', open_options= ['HEADERS=FORCE']) as pwc
    left join countries_iso_data
    on pwc.country = countries_iso_data.country
    where pwc.country is not null
)

'''
)





corp_boricuas_with_orbis_parent_rel = duckdb.sql(
'''
with pr_to_orbis_parent as (
    select company_number_pr, model_data_global_parent_bvdid
    from qualified_companies_pr_orbis_search
    left join orbis_data_results
    on qualified_companies_pr_orbis_search.company_number_pr = orbis_data_results.own_id.ltrim('pr/')
),

corps_boricuas_to_orbis_parent as (
    select company_number_pr, corp_name, model_data_global_parent_bvdid
    from pr_to_orbis_parent
    left join corporaciones_boricuas
    on pr_to_orbis_parent.company_number_pr = corporaciones_boricuas.registration_index
),

companies_in_network as (
    select bvd_id_orbis, company_name, country_iso_code, state
    from orbis_network_results
),

companies_in_network_with_pr as (
    from corps_boricuas_to_orbis_parent
    left join companies_in_network
    on corps_boricuas_to_orbis_parent.model_data_global_parent_bvdid = companies_in_network.bvd_id_orbis
    -- left join orbis_parent_companies
    -- on pr_to_orbis_parent.model_data_global_parent_bvdid = orbis_parent_companies.bvd_id_orbis
)

select
    company_number_pr,
    corp_name as boricua_corp_name,
    model_data_global_parent_bvdid,
    company_name as parent_company_name,
    country_iso_code as parent_country_iso_code,
    state as parent_state,
from companies_in_network_with_pr
where bvd_id_orbis is not null
'''
)
print('corp_boricuas_with_orbis_parent_rel')
print(corp_boricuas_with_orbis_parent_rel)
duckdb.sql(f'''
copy corp_boricuas_with_orbis_parent_rel to '{orbis_results_output_dir}/corp_boricuas_with_orbis_parent_rel.csv'   
''')
print(f"Exported to {orbis_results_output_dir}/corp_boricuas_with_orbis_parent_rel.csv")
print()

display_corp_boricuas_with_orbis_parent_rel = duckdb.sql(
'''
select
    boricua_corp_name,
    parent_company_name,
    parent_country_iso_code || coalesce(' (' || parent_state || ')', '') as parent_country_state,
from corp_boricuas_with_orbis_parent_rel
order by parent_company_name, boricua_corp_name
'''
)
print('display_corp_boricuas_with_orbis_parent_rel')
print(display_corp_boricuas_with_orbis_parent_rel)

# orbis_network_utpr_rel = duckdb.sql(
# '''
# with subsidiaries as (
#     from orbis_network_results
#     where bvd_id_orbis != model_data_global_parent_bvdid
#         and branch_indicator = 'No'
# ),

# parents_with_subsidiary_countries as (
#     select distinct model_data_global_parent_bvdid, country_iso_code as subsidiary_country_code
#     from subsidiaries
#     group by all
# ),

# utpr_calc_sheet as (
#     select
#         model_data_global_parent_bvdid, subsidiary_country_code,
#         country as subsidiary_country, "year",
#         -- pillar2, iir, qdmtt, 
#         utpr,
#         case when utpr = 'Yes' then TRUE when utpr = 'No' then FALSE else NULL end as utpr_bool,
#         list(utpr_bool) over (partition by model_data_global_parent_bvdid, "year") as utpr_bool_list,
#         utpr_bool_list.list_filter(x -> x).len() > 0 as utpr_bool_any
#     from parents_with_subsidiary_countries
#     left join pillar2_pwc_report
#     on parents_with_subsidiary_countries.subsidiary_country_code = pillar2_pwc_report.country_code
    
# )

# select distinct model_data_global_parent_bvdid, "year", utpr_bool_any, case when utpr_bool_any then 'Yes' else 'No' end as utpr_any,
# --select distinct subsidiary_country_code
# from utpr_calc_sheet
# --where "subsidiary_country" is not null -- Eliminar corps que no pudimos match al Pillar2 report
# -- order by model_data_global_parent_bvdid, "year", subsidiary_country_code
# '''
# )
orbis_network_utpr_rel = duckdb.sql(
'''
with subsidiaries as (
    from orbis_network_results
    where bvd_id_orbis != model_data_global_parent_bvdid
        and branch_indicator = 'No'
),

companies_with_tax_country as (
    select *,
        case when state = 'PR' then 'PR' else country_iso_code end
        as tax_country_iso_code,
    from orbis_network_results
),

companies_with_parent_tax_country as (
    select comps_1.*, comps_2.tax_country_iso_code as parent_tax_country_iso_code
    from companies_with_tax_country as comps_1
    left join companies_with_tax_country as comps_2
    on comps_1.model_data_global_parent_bvdid = comps_2.bvd_id_orbis
    where comps_1.model_data_global_parent_bvdid is not null
),

parents_with_subsidiary_countries as (
    select
        model_data_global_parent_bvdid, parent_tax_country_iso_code,
        list(distinct tax_country_iso_code) FILTER (
            bvd_id_orbis != model_data_global_parent_bvdid and branch_indicator = 'No' -- elegir subsidiarias
            and tax_country_iso_code != parent_tax_country_iso_code -- excluir home country (solo mirar internacional del home del parent)
        ) as international_subsidiary_tax_country_codes
    from companies_with_parent_tax_country
    group by model_data_global_parent_bvdid, parent_tax_country_iso_code
),

parents_lacking_subsidiaries as (
    from parents_with_subsidiary_countries
    where international_subsidiary_tax_country_codes is null
),

parents_having_subsidiaries as (
    from parents_with_subsidiary_countries
    where international_subsidiary_tax_country_codes is not null
),

parent_subsidiary_country_combinations as (
    select model_data_global_parent_bvdid, parent_tax_country_iso_code,
        unnest(international_subsidiary_tax_country_codes) as international_subsidiary_tax_country_code
    from parents_having_subsidiaries
),

parents_with_international_subsidiary_utprs as (
    select parent_subsidiary_country_combinations.*,
        pillar2_pwc_report."year",
        pillar2_pwc_report.utpr as international_country_utpr_yesno,
        case when international_country_utpr_yesno = 'Yes' then TRUE when international_country_utpr_yesno = 'No' then FALSE else NULL end as international_country_utpr_bool,
    from parent_subsidiary_country_combinations
    left join pillar2_pwc_report
    on parent_subsidiary_country_combinations.international_subsidiary_tax_country_code = pillar2_pwc_report.country_code
    where pillar2_pwc_report."year" is not null -- ocurre cuando no hay un match entre paises pq no esta disponible la info (ej. RU, TZ, MW)
), 

parents_having_subsidiaries_with_utpr_calc as (
    select
        model_data_global_parent_bvdid, parent_tax_country_iso_code, "year",
        list(international_country_utpr_bool).list_filter(x -> x).len() > 0
            as parent_subject_to_utpr,
    from parents_with_international_subsidiary_utprs
    group by all
),

parents_lacking_subsidiaries_with_utpr_calc as (
select model_data_global_parent_bvdid, parent_tax_country_iso_code,
    unnest([2024, 2025, 2026]) as "year", FALSE as parent_subject_to_utpr
from parents_lacking_subsidiaries
)

from parents_having_subsidiaries_with_utpr_calc
union all by name
from parents_lacking_subsidiaries_with_utpr_calc
order by model_data_global_parent_bvdid, "year"
'''
)
print('orbis_network_utpr_rel')
print(orbis_network_utpr_rel)
print(duckdb.sql('select count() from orbis_network_utpr_rel'))

parent_companies_with_pillar2_status_rel = duckdb.sql(
'''
with parents_with_bori_corps_count as (
    select
    model_data_global_parent_bvdid, parent_company_name,
    count(distinct company_number_pr) as num_corps_boricuas,
    -- parent_country_iso_code,
    case when parent_state = 'PR' then 'PR' else parent_country_iso_code end
        as parent_tax_country_iso_code,

    from corp_boricuas_with_orbis_parent_rel
    group by all
),

parents_with_pwc_data as (
    select parents_with_bori_corps_count.*,
    country as parent_country, "year", pillar2, iir, qdmtt, --utpr
    from parents_with_bori_corps_count
    left join pillar2_pwc_report
    on parents_with_bori_corps_count.parent_tax_country_iso_code = pillar2_pwc_report.country_code
)

-- select parents_with_pwc_data.* replace (utpr_any as utpr)
select parents_with_pwc_data.*, 
    case when parent_subject_to_utpr is TRUE then 'Yes'
         when parent_subject_to_utpr is FALSE then 'No'
         END as utpr
from parents_with_pwc_data

left join orbis_network_utpr_rel
using (model_data_global_parent_bvdid, "year")
-- where utpr_bool_any is null
'''
)
print('parent_companies_with_pillar2_status_rel')
print(parent_companies_with_pillar2_status_rel)
# duckdb.sql(f'''
# copy parent_companies_with_pillar2_status_rel to '{orbis_results_output_dir}/parent_companies_with_pillar2_status_rel.xlsx' WITH (FORMAT GDAL, DRIVER 'xlsx')
# ''')
parent_companies_with_pillar2_status_rel.pl().write_excel(f'{orbis_results_output_dir}/parent_companies_with_pillar2_status_rel.xlsx')
print(f'Exported to {orbis_results_output_dir}/parent_companies_with_pillar2_status_rel.xlsx')

# import sys; sys.exit()

# rel = duckdb.sql(
# '''
# -- from orbis_network_utpr_rel
# --from orbis_network_results
# from parent_companies_with_pillar2_status_rel
# --where model_data_global_parent_bvdid = 'US314879018L'
# --where model_data_global_parent_bvdid = bvd_id_orbis
# '''
# )
# print('rel')
# print(rel)

# El asunto es que entendemos como funciona el caso de UTPR con un wide gamma de subsidiarias
# pero nos trancamos en los base cases. What if no tienes susbidiarias? (e.j. US314879018L que es solo "parent" pero de nadie)
# what if tienes una subsidiaria en tu propio pais? O sea como manejas el pais del parent manejando UTPR

display_parent_companies_with_pillar2_status_2024_rel = duckdb.sql(
'''
select parent_company_name, num_corps_boricuas, parent_tax_country_iso_code, iir, qdmtt,
from parent_companies_with_pillar2_status_rel
where "year" = 2024
order by num_corps_boricuas desc
'''
)
print('display_parent_companies_with_pillar2_status_2024_rel')
print(display_parent_companies_with_pillar2_status_2024_rel)

display_parent_companies_with_pillar2_status_2025_rel = duckdb.sql(
'''
select parent_company_name, num_corps_boricuas, parent_tax_country_iso_code, iir, qdmtt, utpr
from parent_companies_with_pillar2_status_rel
where "year" = 2025
order by num_corps_boricuas desc
'''
)
print('display_parent_companies_with_pillar2_status_2025_rel')
print(display_parent_companies_with_pillar2_status_2025_rel)

rel = duckdb.sql(
'''
-- Ernst & Young (US245251451L) tiene solo US (parent), US (filiales) y BN Brunei Darussalam (filial)
-- so lo que pasa es que no hay info para BN y el US es parent
-- so el UTPR para E&Y de PR aparece vacio (NULL)
-- Solucion: rellenar pwc data para BN

-- Alera Group (US*4000000225888) tiene parent en US y filiales en US y LB Lebanon
-- same as before, no hay info sobre LB en el PWC report
-- Solucion: rellenar pwc data para LB

select list(distinct country_iso_code)
from orbis_network_results
where model_data_global_parent_bvdid = 'US*4000000225888'
'''
)
print('rel')
print(rel)