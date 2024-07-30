# Para determinar cuales son las empresas
# que cruzan el umbral de revenue de 750M de euros

import os
import duckdb

# Recuerda correr primero el script 'merge_orbis_results.py'
orbis_results_name = 'orbis_search_results_2024_07_21' # corresponde al directorio a abrir dentro de 'outputs/'
orbis_results_dir = f'outputs/{orbis_results_name}'

duckdb.sql(
f'''
create view orbis_data_results as (
    select * replace (operating_revenue_year_2024_eur_th::double as operating_revenue_year_2024_eur_th)
    from read_parquet('{orbis_results_dir}/orbis_data_results.parquet')
)
'''
)

qualified_companies_worksheet_rel = duckdb.sql(
'''
select bvd_id_orbis, model_data_global_parent_bvdid, list_value(
    "operating_revenue_year_2024_eur_th", "operating_revenue_year_2023_eur_th", "operating_revenue_year_2022_eur_th", "operating_revenue_year_2021_eur_th",
    "operating_revenue_year_2020_eur_th", "operating_revenue_year_2019_eur_th", "operating_revenue_year_2018_eur_th", "operating_revenue_year_2017_eur_th",
    "operating_revenue_year_2016_eur_th", "operating_revenue_year_2015_eur_th", "operating_revenue_year_2014_eur_th", "operating_revenue_year_last_eur_th",
    "model_data_global_parent_operating_revenue_eur_th")
    as operating_revenue_values,
    operating_revenue_values.list_filter(d -> d is not NULL).len() as num_years_available,
    num_years_available = 0 as no_revenue_data,
    operating_revenue_values.list_aggregate('max') as operating_revenue_max_eur_th,
    operating_revenue_values.list_filter(d -> d > 750e3).len() > 0 as qualified, -- any year above 750M (recuerda valores estan en thousands)
from orbis_data_results
--describe orbis_data_results
'''
)
print('qualified_companies_worksheet_rel')
print(qualified_companies_worksheet_rel)

qualified_parent_companies_rel = duckdb.sql(
'''
with qualified_companies as (
    -- select bvd_id_orbis, model_data_global_parent_bvdid
    from qualified_companies_worksheet_rel
    where qualified = TRUE
)

select model_data_global_parent_bvdid, max(operating_revenue_max_eur_th) as filial_operating_revenue_max_eur_th,
from qualified_companies
group by model_data_global_parent_bvdid
-- left join orbis_data_results
-- using (bvd_id_orbis)
'''
)
print('qualified_parent_companies_rel')
print(qualified_parent_companies_rel)
duckdb.sql(f'''
copy qualified_parent_companies_rel to '{orbis_results_dir}/qualified_parent_companies.csv'        
''')
print(f'Saved to {orbis_results_dir}/qualified_parent_companies.csv')

qualified_company_number_pr_rel = duckdb.sql(
'''
select distinct own_id.ltrim('pr/') as company_number_pr
from qualified_parent_companies_rel
left join orbis_data_results
using (model_data_global_parent_bvdid)
'''
)
print('Qualified companies de PR:')
print('(Identificadores para buscar en OpenCorporates y/o Departamento de Estado)')
print('qualified_company_number_pr_rel')
print(qualified_company_number_pr_rel)

# Export
duckdb.sql(
f'''
copy qualified_company_number_pr_rel to '{orbis_results_dir}/qualified_company_number_pr.csv'
'''
)
print(f'Saved to {orbis_results_dir}/qualified_company_number_pr.csv')