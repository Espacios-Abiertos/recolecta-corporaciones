# Ya sabemos cuales filiales de PR (identificadas por su company number de OpenCorporates y el Dept de Estado)
# hay que buscar en Orbis. Sabemos porque su filial cualificó en la busqueda de Orbis. El ID de la corp boricua
# se rastreó a través de la variable de "own_id" que subimos a Orbis.

# Ahora, tenemos que crear la tabla de busqueda de variables que subiremos
# a Orbis para encontrar las corporaciones de PR en el sistema de Orbis.

import os
import duckdb

from utils_database import read_query

orbis_results_name = 'orbis_search_results_2024_07_21' # corresponde al directorio a abrir dentro de 'outputs/'
orbis_results_dir = f'outputs/{orbis_results_name}'

# duckdb.sql(read_query('attach_db_readonly'))

duckdb.sql(
f'''
create view orbis_data_results as (
    select * replace (operating_revenue_year_2024_eur_th::double as operating_revenue_year_2024_eur_th)
    from read_parquet('{orbis_results_dir}/orbis_data_results.parquet')
)
'''
)

duckdb.sql(
'''
create view companies as (
    select
        results.company.company_number,
        results.company.name,
        results.company.jurisdiction_code,
        results.company.branch,
        
        results.company.registered_address.locality,
        
        results.company.number_of_employees,
        -- results.company.home_company,
        results.company.home_company.name as home_company_name,
        results.company.home_company.jurisdiction_code as home_company_jurisdiction_code,
        results.company.home_company.company_number as home_company_company_number,
        results.company.ultimate_controlling_company,
        
        
        
        results.company.home_company is not null as has_home_company,
        results.company.controlling_entity is not null as has_controlling_entity,
        results.company.ultimate_controlling_company is not null as has_ultimate_controlling_company,
        len(results.company.ultimate_beneficial_owners) > 0 as has_ultimate_beneficial_owners

    -- Esta localizado en el Google Drive so gotta link to that
    from read_json('/Users/gabriel/Google Drive/My Drive/Espacios Abiertos Puerto Rico Inc./Proyectos y herramientas/Our Research & Analytics/Our Research/Global Tax/Data Collections/OpenCorporates - Corporation Info/*.json')
)
'''
)

qualified_companies_pr_rel = duckdb.sql(
f'''
with qualified_company_ids_pr as (

    from '{orbis_results_dir}/qualified_company_number_pr.csv'
)

select company_number, name, jurisdiction_code,
locality.trim().replace(',','') as locality
from qualified_company_ids_pr
left join companies
on qualified_company_ids_pr.company_number_pr = companies.company_number
'''
)
print(qualified_companies_pr_rel)

qualified_companies_pr_orbis_search_rel = duckdb.sql(
'''
select qualified_companies_pr_rel.*,
'US' as country_code,
locality.upper() as locality_upper_case,
-- \'\'\'\' || locality || \'\'\'\' as locality_padded,
-- locality.lower().split(' ').list_transform(x -> upper(x[1]) || x[2:]).list_reduce((x,y) -> x || ' ' || y) as locality_title_case,
orbis_data_results.model_data_global_parent_bvdid,
from qualified_companies_pr_rel
left join orbis_data_results
on qualified_companies_pr_rel.company_number = orbis_data_results.own_id.ltrim('pr/')
--where locality.regexp_matches('[^a-zA-Z\s]') or locality.regexp_matches(' ')
'''
)
print(qualified_companies_pr_orbis_search_rel)

# Export
duckdb.sql(
f'''
copy qualified_companies_pr_orbis_search_rel to '{orbis_results_dir}/qualified_companies_pr_orbis_search.csv'
'''
)
print(f"Exported to {orbis_results_dir}/qualified_companies_pr_orbis_search.csv")