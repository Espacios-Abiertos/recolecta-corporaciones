import os
import re
import polars as pl
import polars.selectors as cs

orbis_results_dir = 'inputs/orbis_results'
orbis_result_files = [
    f for f in os.listdir(orbis_results_dir)
    if f.endswith('.xlsx') and not f.startswith('~')
]
orbis_result_files.sort()
print(orbis_result_files)

orbis_results_over_threshold = []

for fname in orbis_result_files:
    print(f'File: {fname}')

    # fname = orbis_result_files[0]
    fpath = os.path.join(orbis_results_dir, fname)

    bvd_id_sheet = pl.read_excel(fpath, sheet_name='Search summary', read_options={
        'skip_rows': 8,
    })
    bvd_ids = bvd_id_sheet.select('_duplicated_1').to_series().to_list()[0].split(', ')
    bvd_ids = pl.Series(bvd_ids).alias('bvd_id_orbis')
    # print(bvd_id_sheet)
    # print(bvd_ids)
    # STAHP

    orbis_batch = pl.read_excel(fpath, sheet_name='Results', read_options={
        'null_values': ['n.a.' ,'#N/A'],
        'infer_schema_length': 10_000,
        'schema_overrides': {
            'P/L before tax\nth USD Year - 1': pl.Float64,
            'P/L before tax\nth USD Year - 2': pl.Float64,
            'P/L before tax\nth USD Year - 3': pl.Float64,
            'P/L before tax\nth USD Year - 4': pl.Float64,
            'Total assets\nth USD Year - 1': pl.Float64,
            'Total assets\nth USD Year - 2': pl.Float64,
            'Total assets\nth USD Year - 3': pl.Float64,
            'Total assets\nth USD Year - 4': pl.Float64,
        }
    })
    orbis_batch.insert_column(0, bvd_ids)

    if 'Operating revenue (Turnover)\nth EUR Last avail. yr' not in orbis_batch.columns:
        # print(orbis_batch.columns)
        if 'Operating revenue (Turnover)\nEUR Last avail. yr' not in orbis_batch.columns:
            raise ValueError('No hay columna de ingresos en euros')
        nominal_cols = [
            c for c in orbis_batch.columns
            if re.match(r'^Operating revenue \(Turnover\)\nEUR', c)
        ]
        orbis_batch = (
            orbis_batch
            .with_columns(
            *[
                (pl.col(c) / 1e3).cast(pl.Float64).alias(c.replace('EUR', 'th EUR'))
                for c in nominal_cols
            ]
            )
        )
        # raise NotImplementedError('No hay columna de ingresos')
    
    orbis_batch = (
        orbis_batch
        .select(pl.lit(fname).alias('file_name'), *orbis_batch.columns)
        .drop('')
        .rename({
            'Company name Latin alphabet': 'company_name',
            'Country ISO code': 'country_code',
            'State or province (in US or Canada)': 'state',

            'Operating revenue (Turnover)\nth EUR Last avail. yr': 'operating_revenue_year_last_eur',
            'Operating revenue (Turnover)\nth EUR Year - 1': 'operating_revenue_year_1_eur',
            'Operating revenue (Turnover)\nth EUR Year - 2': 'operating_revenue_year_2_eur',
            'Operating revenue (Turnover)\nth EUR Year - 3': 'operating_revenue_year_3_eur',
            'Operating revenue (Turnover)\nth EUR Year - 4': 'operating_revenue_year_4_eur',
        })
        .select([
            'file_name', 'bvd_id_orbis', 'company_name', 'country_code', 'state',
            'operating_revenue_year_last_eur',
            'operating_revenue_year_1_eur',
            'operating_revenue_year_2_eur',
            'operating_revenue_year_3_eur',
            'operating_revenue_year_4_eur',
        ])
    )

    orbis_batch = (
        orbis_batch
        .with_columns(
            operating_revenue_yearly_values=pl.concat_list(cs.matches('operating_revenue_year_\d_eur'))
        )
        .with_columns(
            operating_revenue_num_years_available=pl.col('operating_revenue_yearly_values').list.drop_nulls().list.len(),
            operating_revenue_num_years_over_threshold=pl.col('operating_revenue_yearly_values').list.eval(pl.element() >= 750e3).list.sum(),
        )
        .with_columns(
            operating_revenue_over_threshold=
            pl.when(pl.col('operating_revenue_num_years_available') >= 2)
            .then(pl.col('operating_revenue_num_years_over_threshold') >= 2)
            .otherwise(
                (pl.col('operating_revenue_year_last_eur') >= 750e3).fill_null(False)
            )
        )
    )
    # print(orbis_batch)
    # print(orbis_batch.columns)

    orbis_batch_over_threshold = orbis_batch.filter(pl.col('operating_revenue_over_threshold'))
    # print(orbis_batch_over_threshold)
    print(f'{orbis_batch_over_threshold.height} companies over threshold')

    orbis_results_over_threshold.append(orbis_batch_over_threshold)
    print()


orbis_results_over_threshold: pl.DataFrame = pl.concat(orbis_results_over_threshold)
print(orbis_results_over_threshold)

orbis_results_over_threshold.write_excel('outputs/orbis_results_over_threshold.xlsx')
(orbis_results_over_threshold
 .select(pl.exclude('operating_revenue_yearly_values'))
 ).write_csv('outputs/orbis_results_over_threshold.csv')
orbis_results_over_threshold.write_parquet('outputs/orbis_results_over_threshold.parquet')