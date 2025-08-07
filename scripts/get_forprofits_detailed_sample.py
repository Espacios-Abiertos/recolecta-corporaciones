import duckdb
from utils_database import read_query

con = duckdb.connect(':memory:') # maybe change later?
con.execute(read_query('utils'))
con.execute(read_query('attach_db_readonly'))
for q in read_query('apareo_decretos_corporaciones').split(';'):
    con.execute(q)

# Leer resultado de `scripts/get_forprofits.py`
forprofits_rel = con.sql(
'''
select *
from 'outputs/for_profits.parquet'
'''
)
print('Compañías con fines de lucro:')
forprofits_rel.show()


# Tomar una muestra de 5% de las compañías
# Ver documentacion en https://duckdb.org/docs/stable/sql/samples
forprofits_sample_rel = con.sql(
'''
with ids_sample as (
    select registration_index
    from forprofits_rel
    using sample 5% (system, 1) -- Using "system" sampling and 1 as seed for consistency
)

select *
from ids_sample
left join forprofits_rel
using (registration_index)
'''
)
print(f"Muestra de {len(forprofits_sample_rel):,.0f} compañías con fines de lucro:")
forprofits_sample_rel.show()


# # Guardar la muestra en formato Excel y Parquet
# forprofits_sample_rel.pl().write_excel('outputs/forprofits_sample.xlsx')
# forprofits_sample_rel.pl().write_parquet('outputs/forprofits_sample.parquet')

# Leer datos detallados (segun disponible)
# Basado en archivos dentro de 'recolectas_incrementales/corporation_info/'
companies_detailed_rel = con.sql(
'''
    select response.corporation.corpRegisterIndex as register_index,
           response.corporation.corpName as corp_name,
           response.corporation.statusEs as status,
           response.corporation.dateFormed as date_formed,
           response.corporation.expirationDate as date_expiration,
           response.corporation.terminationDate as date_termination,
           
           -- Otros ejemplos:
           -- response.corporation.classEn as class_en,
           -- response.corporation.jurisdictionEs as jurisdiction,
           -- response.corporation.corporationJurisdictionTypeId as jurisdiction_id,
           -- response.mainLocation.foreignDomicileAddress.city::VARCHAR as foreign_city,
           --  response.mainLocation.streetAddress.city as pr_city,

           --  CASE WHEN jurisdiction_id = 2 THEN coalesce(foreign_city, pr_city)
           --  ELSE NULL END
           --  as city,

           --  CASE WHEN jurisdiction_id = 2 THEN 'United States'
           --  ELSE
           --  coalesce(response.corporation.homeState::VARCHAR, response.mainLocation.foreignDomicileAddress.province::VARCHAR)
           --  END
           --  as country_name,
            
           --  CASE WHEN jurisdiction_id = 2 THEN 'US'
           --  ELSE
           --  NULL
           --  END
           --  as country_code,
           
           'https://rcp.estado.pr.gov/en/entity-information?c=' || register_index as enlace
    from read_json('recolectas_incrementales/corporation_info/*.json')
'''
)
print('Ejemplo de tabla de compañías detalladas:')
con.sql('from companies_detailed_rel limit 10').show()

forprofits_detailed_sample_rel = con.sql(
'''
select forprofits_sample_rel.registration_index, companies_detailed_rel.*
from forprofits_sample_rel
left join companies_detailed_rel
on forprofits_sample_rel.registration_index = companies_detailed_rel.register_index
-- NOTE: The column names for the registration ID variable is different across both tables
-- but it helps to tell apart which values are missing and must be downloaded from the Dept of State site
'''
)
print(f"Muestra detallada de {len(forprofits_detailed_sample_rel):,.0f} compañías con fines de lucro:")
forprofits_detailed_sample_rel.show()

print('Compañías en la muestra sin información detallada:')
con.sql(
'''
select *
from forprofits_detailed_sample_rel
where register_index is null
'''
).show()