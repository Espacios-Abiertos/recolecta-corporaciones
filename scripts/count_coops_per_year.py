import duckdb
from utils_database import read_query

con = duckdb.connect(':memory:') # maybe change later?
con.execute(read_query('utils'))
con.execute(read_query('attach_db_readonly'))
for q in read_query('apareo_decretos_corporaciones').split(';'):
    con.execute(q)

read_corporaciones = False
# 10.5 minutes for 178,550 JSON files in recolectas_incrementales/corporation_info/*.json (o sea really slow)
if read_corporaciones:
    print('Leyendo corporaciones...')
    con.execute(r'''
    copy (
    with coop_corps as (
        select registration_index
        from corporaciones
        where corp_name ilike '%coop%'
    ),

    corp_info as (
    select
        response.corporation.corpRegisterIndex as registration_index,
        response.corporation.corpName as corporation_name,
        
        response.corporation.effectiveDate::VARCHAR as date_effective,
        response.corporation.dateFormed::VARCHAR as date_formed,
        response.corporation.expirationDate::VARCHAR  as date_expiration,
        response.corporation.terminationDate::VARCHAR as date_termination,
        
    from read_json('recolectas_incrementales/corporation_info/*.json')
    -- limit 2000
    ),

    coop_corps_with_info as (
    from coop_corps
    left join corp_info
    using (registration_index)
    )

    select registration_index, corporation_name,
        replace(columns('date_*'), '"', '')
        .regexp_extract('([0-9]{4})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])')
        .strptime('%Y-%m-%dT%H:%M:%S')
    from corp_info
    ) to 'outputs/corporaciones_fechas.parquet'
    ''')
    print('Guardadas corporaciones en outputs/corporaciones_fechas.parquet')

cooperativas_fechas_rel = con.sql('''
with corporaciones_fechas as (
    from 'outputs/corporaciones_fechas.parquet'
),

cooperativas_fechas as (
    from corporaciones_fechas
    where corporation_name ilike '%coop%'
)


from cooperativas_fechas
''')
cooperativas_fechas_rel.show()
con.execute('''
copy cooperativas_fechas_rel to 'outputs/cooperativas_fechas.parquet'
''')

cooperativas_fechas_rel.pl().write_excel('outputs/cooperativas_fechas.xlsx')
print('Guardadas cooperativas_fechas en outputs/cooperativas_fechas.xlsx y parquet')

event_types_by_year_rel = con.sql('''
with cooperativas_fechas as (
    from 'outputs/cooperativas_fechas.parquet'
),

unpivot_subset as (
select registration_index, columns('date_*')
from cooperativas_fechas
),

event_list_raw as (

unpivot unpivot_subset
on columns('date_*')
into
    name event_name
    value event_date
),

event_list as (
select * replace (
    event_name.regexp_replace('(date_)', '') as event_name
    ), date_part('year', event_date) as event_year
from event_list_raw
),

event_list_by_year_type as (
    select event_year, event_name, count() as num_cooperativas
    from event_list
    group by all
),

event_types_by_year as (

pivot event_list_by_year_type
on event_name
using sum(num_cooperativas)
order by event_year
)

from event_types_by_year
''')

print('event_types_by_year')
event_types_by_year_rel.show()

event_types_by_year_rel.pl().write_excel('outputs/cooperativas_event_types_by_year.xlsx')
print('Saved to outputs/cooperativas_event_types_by_year.xlsx')