# Para aparear los nombres de los approved decrees
# con los registration_index (los IDs) de
# las entidades del Registro de Corporaciones

import duckdb
import polars as pl

# Importar los decretos aprobados
decrees = pl.read_parquet('outputs/decrees.parquet')

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
                                         how='left', coalesce=True)
print(decrees_con_corporaciones)
print(decrees_con_corporaciones
      .group_by(pl.col('registration_index').is_not_null())
      .len()
)