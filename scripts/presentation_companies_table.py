# Creado en: 2024-08-02
# Para formatear una tabla para ser mas presentation friendly
# sin tener que pelearle a los Pivot table en Excel

import os

import duckdb
import polars as pl
from utils_database import read_query

orbis_results_name = 'orbis_network_results_2024_07_23' # corresponde al directorio a abrir dentro de 'outputs/'
orbis_results_output_dir = f'outputs/{orbis_results_name}'

duckdb.sql(f'''
create view corp_boricuas_with_orbis_parent_rel as (
from '{orbis_results_output_dir}/corp_boricuas_with_orbis_parent_rel.csv'
)
''')

corp_boricuas_with_orbis_parent_rel = duckdb.sql(
'''
from corp_boricuas_with_orbis_parent_rel
'''
)
print('corp_boricuas_with_orbis_parent_rel')
print(corp_boricuas_with_orbis_parent_rel)
print()

parent_ranking_rel = duckdb.sql(
'''
with initial as (
    select
        model_data_global_parent_bvdid,
        
        filial_operating_revenue_max_eur_th,
    from corp_boricuas_with_orbis_parent_rel
    group by all
)

select 
row_number() over (order by filial_operating_revenue_max_eur_th desc) as parent_rank,
*,
from initial
order by parent_rank
'''
)
print('parent_ranking_rel')
print(parent_ranking_rel)
print()

duckdb.sql('''SET default_null_order = 'NULLS FIRST' ''')
companies_table_rel = duckdb.sql(
'''
with corps_with_null_duplicates as (
    from (select distinct model_data_global_parent_bvdid, parent_company_name, parent_country_iso_code, parent_state, filial_operating_revenue_max_eur_th
    from corp_boricuas_with_orbis_parent_rel)
    union all by name
    from corp_boricuas_with_orbis_parent_rel
)


select
    parent_rank,
    row_number() over (partition by model_data_global_parent_bvdid order by boricua_corp_name) as bori_corp_row_number,
    model_data_global_parent_bvdid, parent_company_name, parent_country_iso_code, parent_state, corp_boricuas_with_orbis_parent_rel.filial_operating_revenue_max_eur_th,
    company_number_pr, boricua_corp_name,
from corp_boricuas_with_orbis_parent_rel
left join parent_ranking_rel
using (model_data_global_parent_bvdid)
order by parent_rank, bori_corp_row_number
'''
)
print('companies_table_rel')
print(companies_table_rel)
print()

display_companies_table_rel = duckdb.sql('''
select
    parent_rank as "Número de matriz",
    bori_corp_row_number as "Número de filial puertorriqueña",
    model_data_global_parent_bvdid as "ID Orbis de matriz",
    parent_company_name as "Nombre de matriz",
    parent_country_iso_code as "País de matriz",
    parent_state as "Estado de matriz",
    filial_operating_revenue_max_eur_th as "Ingresos máximos de matriz (EUR miles)",
    company_number_pr as "ID de compañía de PR",
    boricua_corp_name as "Nombre de filial puertorriqueña",
from companies_table_rel                                       
''')
duckdb.sql(f'''
copy display_companies_table_rel to '{orbis_results_output_dir}/display_companies_table.csv'   
''')
duckdb.sql('load spatial')
duckdb.sql(f'''
copy display_companies_table_rel to '{orbis_results_output_dir}/display_companies_table.xlsx' WITH (FORMAT GDAL, DRIVER 'xlsx') 
''')