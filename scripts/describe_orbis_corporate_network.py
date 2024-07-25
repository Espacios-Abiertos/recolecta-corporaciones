# Creado en 2024-07-23
# Obtuvimos ~900 compañias que cualifican para el Global Tax, y dentro de eso como ~750 parents correspondientes.
# Estas parents fueron lanzadas al buscador de Orbis de corporate structure.
# Resulto en un archivo conteniendo decenas de miles de compañías que representan todas las filiales
# de las compañías parent que lanzamos al buscador.

# En otras palabras, tenemos un tipo de "graph" de las compañías que cualifican para el Global Tax.
# Este script esta encargado de describir las cadenas de compañías y caracterizarlas para determinar
# la presencia de Puerto Rico y tomar siguientes pasos.

import os
import duckdb
import polars as pl

orbis_results_input_dir = 'inputs/orbis_network_results_2024_07_23'
results_name = orbis_results_input_dir.lstrip('inputs/')
orbis_results_output_dir = f'outputs/{results_name}'
os.makedirs(orbis_results_output_dir, exist_ok=True)

reexport_excel = False # Set to True if you want to reexport the excel to parquet
# es que toma demasiado tiempo para leer el excel y no quiero hacerlo cada vez que corro el script
if reexport_excel:
    fname_in = 'CONSOLIDATED - Corporate Group for Parent Companies.xlsx' # Puede cambiar segun Arturo haga rename
    # Read in the data
    orbis_network_results = (
        pl.read_excel(f'{orbis_results_input_dir}/{fname_in}')
        .rename({
            '': 'row_number',
            'Company name Latin alphabet': 'company_name',
            'BvD ID number': 'bvd_id_orbis',
            'Model data - Global parent bvdid': 'model_data_global_parent_bvdid',
            'Branch indicator': 'branch_indicator',
            'Country': 'country',
            'Country ISO code': 'country_iso_code',
            'Region in country': 'region_in_country',
            'State or province (in US or Canada)': 'state',
        })
    )
    print(orbis_network_results)

    # Write to parquet
    orbis_network_results.write_parquet(f'{orbis_results_output_dir}/orbis_network_results.parquet')
    print(f'Wrote to {orbis_results_output_dir}/orbis_network_results.parquet')

duckdb.sql(
f'''
create view orbis_network_results as (
    from read_parquet('{orbis_results_output_dir}/orbis_network_results.parquet')
)
'''
)

rel = duckdb.sql(
'''
select
    count() as num_rows,
    count(distinct bvd_id_orbis) as num_unique_companies,
    count(distinct bvd_id_orbis) FILTER (state = 'PR') as num_unique_companies_with_pr_state,
    count(distinct model_data_global_parent_bvdid) as num_unique_parents,
    count(distinct model_data_global_parent_bvdid) FILTER (state = 'PR') as num_unique_parents_with_pr_state,
from orbis_network_results
'''
)
print(rel)