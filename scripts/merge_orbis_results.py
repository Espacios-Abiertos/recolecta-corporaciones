# Creado en: 2024-07-22
# Background: Acabamos de tirar 8 batches de busquedas a Orbis por segunda vez.
# Esta vez, sí incluimos los IDs de OpenCorporate como "Own ID" durante la busqueda.
# De tal manera, preservamos la relación entre las corporaciones de OpenCorporates y los resultados de Orbis.
# Por eso hay dos Excel per batch: uno de SEARCH que tiene los Own ID y los de DATA que tiene los resultados de Orbis.

import os
import duckdb
import polars as pl

from operator import itemgetter

orbis_results_dir = 'inputs/orbis_search_results_2024_07_21'
results_name = orbis_results_dir.lstrip('inputs/')

results_output_dir = f'outputs/{results_name}'
os.makedirs(results_output_dir, exist_ok=True)

orbis_results_files_rel = duckdb.sql(
f'''
with initial as (
select
    file as fpath, fpath.parse_filename() as fname,
    fname.regexp_extract('\((\w+) with pr pill\) (\d+)-(\d+)', ['dataset_type', 'query_num_start', 'query_num_end']) as fmeta,
    fmeta.struct_extract('dataset_type') as dataset_type,
    fmeta.struct_extract('query_num_start')::INT as query_num_start,
    fmeta.struct_extract('query_num_end')::INT as query_num_end,
    query_num_start::VARCHAR || '-' || query_num_end::VARCHAR as query_range,
from glob('{orbis_results_dir}/*.xlsx')
order by dataset_type, query_num_start
) from initial select * exclude (fmeta) where not starts_with(fname, '~$')
'''
)

orbis_results_files = orbis_results_files_rel.pl()
print(orbis_results_files)

query_ranges_rel = duckdb.sql(
'''
with initial as (
    select distinct on (query_range) query_range, query_num_start
    from orbis_results_files_rel
)
select list(query_range order by query_num_start) as query_ranges from initial
'''
)
# print(query_ranges_rel)
query_ranges = query_ranges_rel.fetchone()[0]
print(query_ranges)

orbis_search_results = None
# Merge search files
for search_result_file in orbis_results_files.filter(dataset_type=pl.lit('SEARCH')).iter_rows(named=True):
    fname, fpath = itemgetter('fname', 'fpath')(search_result_file)
    print(fname)

    search_result_batch = (
        pl.read_excel(fpath)
        .with_columns(pl.lit(fname).alias('file_name'))
        # .with_row_index(name='row_in_file', offset=1)
    )
    # print(search_result_batch)

    if orbis_search_results is None:
        orbis_search_results = search_result_batch
    else:
        orbis_search_results = pl.concat([orbis_search_results, search_result_batch])

orbis_search_results = (
    orbis_search_results
    .with_row_index(name='row_number', offset=1)
    .rename({
        'Own ID': 'own_id',
        'Company name': 'company_name',
        'City': 'city',
        'Country': 'country',
        'Identifier': 'identifier',
        'Score': 'score',
        'Matched BvD ID': 'matched_bvd_id',
        'Matched company name': 'matched_company_name',
    })
)
print(orbis_search_results)
print(orbis_search_results.columns)


orbis_search_results.write_excel(f'{results_output_dir}/orbis_search_results.xlsx')
orbis_search_results.write_parquet(f'{results_output_dir}/orbis_search_results.parquet')

orbis_data_results = None
# Merge data files
for data_result_file in orbis_results_files.filter(dataset_type=pl.lit('DATA')).iter_rows(named=True):
    fname, fpath = itemgetter('fname', 'fpath')(data_result_file)
    print(fname)

    data_result_batch = (
        pl.read_excel(fpath, sheet_name='Results', read_options={
            'null_values': ['n.a.']
        })
        .with_columns(pl.lit(fname).alias('file_name'))
        # .with_row_index(name='row_in_file', offset=1)
    )
    # print(data_result_batch)

    if orbis_data_results is None:
        orbis_data_results = data_result_batch
    else:
        orbis_data_results = pl.concat([orbis_data_results, data_result_batch])

# ['row_number', 'row_in_file', 'Company name Latin alphabet', 'BvD ID number', 'Model data - Global parent bvdid', 'City\nLatin Alphabet', 'State or province (in US or Canada)', 'Country', 'Postcode\nLatin Alphabet', 'NAICS 2022, core code (4 digits)', 'NAICS 2022, core code - description', 'Operating revenue (Turnover)\nth EUR 2024', 'Operating revenue (Turnover)\nth EUR 2023', 'Operating revenue (Turnover)\nth EUR 2022', 'Operating revenue (Turnover)\nth EUR 2021', 'Operating revenue (Turnover)\nth EUR 2020', 'Operating revenue (Turnover)\nth EUR 2019', 'Operating revenue (Turnover)\nth EUR 2018', 'Operating revenue (Turnover)\nth EUR 2017', 'Operating revenue (Turnover)\nth EUR 2016', 'Operating revenue (Turnover)\nth EUR 2015', 'Operating revenue (Turnover)\nth EUR 2014', 'Operating revenue (Turnover)\nth EUR Last avail. value', 'Closing date\nDate of the last available value for Operating revenue (Turnover)', 'Model data - Global parent operating revenue\nth EUR', 'file_name']
orbis_data_results = (
    orbis_data_results
    # .with_row_index(name='row_number', offset=1)
    # .with_columns(
    #     pl.int_range(pl.len(), dtype=pl.UInt32).alias("row_number"),
    # )
    .select(pl.exclude(''))
    .rename({
        'Company name Latin alphabet': 'company_name',
        'BvD ID number': 'bvd_id_orbis',
        'City\nLatin Alphabet': 'city',
        'Country': 'country',
        'State or province (in US or Canada)': 'state',
        'Postcode\nLatin Alphabet': 'postcode',
        'Operating revenue (Turnover)\nth EUR Last avail. value': 'operating_revenue_year_last_eur_th',
        'Operating revenue (Turnover)\nth EUR 2024': 'operating_revenue_year_2024_eur_th',
        'Operating revenue (Turnover)\nth EUR 2023': 'operating_revenue_year_2023_eur_th',
        'Operating revenue (Turnover)\nth EUR 2022': 'operating_revenue_year_2022_eur_th',
        'Operating revenue (Turnover)\nth EUR 2021': 'operating_revenue_year_2021_eur_th',
        'Operating revenue (Turnover)\nth EUR 2020': 'operating_revenue_year_2020_eur_th',
        'Operating revenue (Turnover)\nth EUR 2019': 'operating_revenue_year_2019_eur_th',
        'Operating revenue (Turnover)\nth EUR 2018': 'operating_revenue_year_2018_eur_th',
        'Operating revenue (Turnover)\nth EUR 2017': 'operating_revenue_year_2017_eur_th',
        'Operating revenue (Turnover)\nth EUR 2016': 'operating_revenue_year_2016_eur_th',
        'Operating revenue (Turnover)\nth EUR 2015': 'operating_revenue_year_2015_eur_th',
        'Operating revenue (Turnover)\nth EUR 2014': 'operating_revenue_year_2014_eur_th',
        'NAICS 2022, core code (4 digits)': 'naics_2022_core_code',
        'NAICS 2022, core code - description': 'naics_2022_core_code_description',
        'Closing date\nDate of the last available value for Operating revenue (Turnover)': 'closing_date_operating_revenue',
        'Model data - Global parent bvdid': 'model_data_global_parent_bvdid',
        'Model data - Global parent operating revenue\nth EUR': 'model_data_global_parent_operating_revenue_eur_th',
    })
    
)

data_filename_series = orbis_data_results['file_name']
orbis_data_results = orbis_data_results.drop('file_name')
orbis_data_results.insert_column(0, data_filename_series)




# Add own_id from search_results
bvd_own_id_pairs = (
    orbis_search_results
    .select('matched_bvd_id', 'own_id')
    .filter(pl.col('matched_bvd_id').is_not_null())
    .rename({
        'matched_bvd_id': 'bvd_id_orbis',
    })
)

orbis_data_results = (
    orbis_data_results
    .join(bvd_own_id_pairs, on='bvd_id_orbis', how='left', coalesce=True)
)

own_id_series = orbis_data_results['own_id']
orbis_data_results = orbis_data_results.drop('own_id')
orbis_data_results.insert_column(1, own_id_series)

print(orbis_data_results)
print(orbis_data_results.columns)

orbis_data_results.write_excel(f'{results_output_dir}/orbis_data_results.xlsx')
orbis_data_results.write_parquet(f'{results_output_dir}/orbis_data_results.parquet')

