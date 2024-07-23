# Los resultados de la busqueda de Orbis
# no incluyen el termino de busqueda original.
# Por tanto, tenemos que reconectar la corporacion de OpenCorporates
# usada como termino de busqueda con los resultados de Orbis.

import duckdb

from utils_database import read_query

con = duckdb.connect(':memory:')
con.execute(read_query('utils'))

con.execute(
'''
create or replace view orbis_results as (
    select bvd_id_orbis,
        orbis.company_name as company_name_orbis, orbis.country_code as country_code_orbis,
        orbis.state as state_orbis,
    from read_parquet('outputs/orbis_results_over_threshold.parquet') as orbis -- only match companies over threshold
)
'''
)
con.execute(
'''
create or replace view searched_companies as (
    select distinct on (company_number_search, jurisdiction_code_search, company_name_search)
    search_company_company_number as company_number_search,
    search_company_jurisdiction_code as jurisdiction_code_search,

    search_company_name as company_name_search, search_company_country_code as country_code_search,
    search_company_jurisdiction_code.split('_')[2].upper() as state_search,
    from read_parquet('outputs/search_companies_for_orbis.parquet')
)
'''
)


# rel = con.sql(
# '''
# select bvd_id_orbis,
#     orbis.company_name as company_name_orbis, orbis.country_code as country_code_orbis,
#     orbis.state as state_orbis,
# from orbis_results_over_threshold as orbis
# '''
# )
rel = con.sql(
'''
from orbis_results
left join searched_companies
on preprocess_text(orbis_results.company_name_orbis) = preprocess_text(searched_companies.company_name_search)
where company_name_search is null
'''
)
print(rel)

con.close()