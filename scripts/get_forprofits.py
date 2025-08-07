import duckdb
from utils_database import read_query

con = duckdb.connect(':memory:') # maybe change later?
con.execute(read_query('utils'))
con.execute(read_query('attach_db_readonly'))
for q in read_query('apareo_decretos_corporaciones').split(';'):
    con.execute(q)

con.execute('''
create or replace table corporaciones_detalladas as (
    select distinct registration_index, corp_name, status_id, status_es, class_es, class_en, profit_type_es
    from recolecta_corporaciones.recolecta_buscador__records
    order by record_id
)
''')

for_profits_rel = con.sql('''
from corporaciones_detalladas
where profit_type_es = 'Con Fines'
''')

# Guardar archivos en formato Excel y Parquet
for_profits_rel.pl().write_excel('outputs/for_profits.xlsx')
for_profits_rel.pl().write_parquet('outputs/for_profits.parquet')

print('Saved to outputs/for_profits')