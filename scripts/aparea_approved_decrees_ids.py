# Para aparear los nombres de los approved decrees
# con los registration_index (los IDs) de
# las entidades del Registro de Corporaciones

import duckdb
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
)
print('Decrees:')
print(decrees)
print('# de decretos:', decrees.height)

print()

# Importar los nombres de las compañías en el Registro de Corporaciones
duckdb.install_extension('sqlite')
con = duckdb.connect('recolecta_corporaciones.db', read_only=True)
corporaciones = con.sql(
    '''
    select distinct registration_index, corp_name
    from recolecta_buscador__records
    order by record_id
    ''').pl()
print('Corporaciones:')
print(corporaciones)
con.close()

print('Merge:')
decrees_con_corporaciones = decrees.join(corporaciones,
                                         left_on='grantee', right_on='corp_name',
                                         how='left')
print(decrees_con_corporaciones)
print(decrees_con_corporaciones
      .group_by(pl.col('registration_index').is_not_null())
      .len()
)