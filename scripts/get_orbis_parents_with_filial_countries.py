# Creado: 2024-07-24
# Ya tenemos el network de corporaciones de Orbis descargado y listo para analizar (ver 'describe_orbis_corporate_network.py').
# Ahora vamos a hacer un resumen de las parent companies incluyendo: pais de la parent, paises de filiales

import os

import duckdb
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

orbis_network_utpr_rel = duckdb.sql(
'''
with subsidiaries as (
    from orbis_network_results
    where bvd_id_orbis != model_data_global_parent_bvdid
        and branch_indicator = 'No'
),

parents_with_subsidiary_countries as (
    select distinct model_data_global_parent_bvdid, country_iso_code as subsidiary_country_code
    from subsidiaries
    group by all
),

utpr_calc_sheet as (
    select
        model_data_global_parent_bvdid, subsidiary_country_code,
        country as subsidiary_country, "year",
        -- pillar2, iir, qdmtt, 
        utpr,
        case when utpr = 'Yes' then TRUE when utpr = 'No' then FALSE else NULL end as utpr_bool,
        list(utpr_bool) over (partition by model_data_global_parent_bvdid, "year") as utpr_bool_list,
        utpr_bool_list.list_filter(x -> x).len() > 0 as utpr_bool_any
    from parents_with_subsidiary_countries
    left join pillar2_pwc_report
    on parents_with_subsidiary_countries.subsidiary_country_code = pillar2_pwc_report.country_code
    
)

select distinct model_data_global_parent_bvdid, "year", utpr_bool_any, case when utpr_bool_any then 'Yes' else 'No' end as utpr_any,
--select distinct subsidiary_country_code
from utpr_calc_sheet
--where "subsidiary_country" is not null -- Eliminar corps que no pudimos match al Pillar2 report
-- order by model_data_global_parent_bvdid, "year", subsidiary_country_code
'''
)
print('orbis_network_utpr_rel')
print(orbis_network_utpr_rel)

parent_companies_with_pillar2_status_rel = duckdb.sql(
'''
with parents_with_bori_corps_count as (
    select
    model_data_global_parent_bvdid, parent_company_name,
    count(distinct company_number_pr) as num_corps_boricuas,
    parent_country_iso_code,

    from corp_boricuas_with_orbis_parent_rel
    group by all
),

parents_with_pwc_data as (
    select parents_with_bori_corps_count.*,
    country as parent_country, "year", pillar2, iir, qdmtt, utpr
    from parents_with_bori_corps_count
    left join pillar2_pwc_report
    on parents_with_bori_corps_count.parent_country_iso_code = pillar2_pwc_report.country_code
)

-- select parents_with_pwc_data.* replace (utpr_any as utpr)
from parents_with_pwc_data

--left join orbis_network_utpr_rel
--using (model_data_global_parent_bvdid, "year")
-- where utpr_bool_any is null
'''
)
print('parent_companies_with_pillar2_status_rel')
print(parent_companies_with_pillar2_status_rel)

rel = duckdb.sql(
'''
-- from orbis_network_utpr_rel
--from orbis_network_results
from parent_companies_with_pillar2_status_rel
--where model_data_global_parent_bvdid = 'US314879018L'
--where model_data_global_parent_bvdid = bvd_id_orbis
'''
)
print('rel')
print(rel)

# display_parent_companies_with_pillar2_status_2024_rel = duckdb.sql(
# '''
# select parent_company_name, num_corps_boricuas, parent_country_iso_code, iir, qdmtt,
# from parent_companies_with_pillar2_status_rel
# where "year" = 2024
# order by num_corps_boricuas desc
# '''
# )
# print('display_parent_companies_with_pillar2_status_2024_rel')
# print(display_parent_companies_with_pillar2_status_2024_rel)

# display_parent_companies_with_pillar2_status_2025_rel = duckdb.sql(
# '''
# select parent_company_name, num_corps_boricuas, parent_country_iso_code, iir, qdmtt, 'TODO' as utpr
# from parent_companies_with_pillar2_status_rel
# where "year" = 2025
# order by num_corps_boricuas desc
# '''
# )
# print('display_parent_companies_with_pillar2_status_2025_rel')
# print(display_parent_companies_with_pillar2_status_2025_rel)

