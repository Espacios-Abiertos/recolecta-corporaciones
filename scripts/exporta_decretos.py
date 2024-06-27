# Para crear y exportar el archivo de decretos aprobados
# para uso en otros códigos

import polars as pl

# Importar los nombres de las compañías con decretos aprobados
approved_decrees_fpath = 'inputs/List of Approved Decrees.xlsx'
decrees_135_1997 = (
    pl.read_excel(approved_decrees_fpath, sheet_name='Ley 135 1997')
    .with_columns(decree=pl.lit('Ley 135-1997'))
)
decrees_73_2008 = (
    pl.read_excel(approved_decrees_fpath, sheet_name='Ley 73 2008')
    .with_columns(decree=pl.lit('Ley 73-2008'))
)

decrees = (
    pl.concat([decrees_135_1997, decrees_73_2008])
    .rename({
        'Grantee': 'grantee',
        "Decree's Approval Date": 'approval_date',
    })
    .select('decree','grantee','approval_date')
    .with_row_index(name='decree_id', offset=1)
)
print('Decrees:')
print(decrees)
print('# de decretos:', decrees.height)

print()

decrees.write_parquet('outputs/decrees.parquet')
print('Decrees saved to outputs/decrees.parquet')