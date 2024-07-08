import duckdb
import polars as pl

con = duckdb.connect(':memory:')
con.execute('''
create or replace view foraneas_activas as (
    select response.corporation.corpRegisterIndex as register_index,
           response.corporation.corpName as corp_name, response.corporation.jurisdictionEs as jurisdiction,
           response.corporation.statusEs as status, 'https://rcp.estado.pr.gov/en/entity-information?c=' || register_index as enlace
    from read_json('recolectas_incrementales/corporation_info/*.json')
    where (jurisdiction = 'Foránea' or jurisdiction = 'Foránea - No Estadounidense') and status = 'ACTIVA'
    order by jurisdiction, corp_name
)         
''')

foraneas_activas = con.sql('select * from foraneas_activas').pl()

foraneas_activas.write_excel('outputs/foraneas_activas.xlsx')