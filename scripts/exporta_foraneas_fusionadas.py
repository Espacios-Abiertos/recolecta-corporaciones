import duckdb
import polars as pl

con = duckdb.connect(':memory:')
con.execute('''
create or replace view foraneas_fusionadas as (
    select response.corporation.corpRegisterIndex as register_index,
           response.corporation.corpName as corp_name,
           response.corporation.jurisdictionEs as jurisdiction,
           response.corporation.corporationJurisdictionTypeId as jurisdiction_id,
           response.mainLocation.foreignDomicileAddress.city::VARCHAR as foreign_city,
            response.mainLocation.streetAddress.city as pr_city,

            CASE WHEN jurisdiction_id = 2 THEN coalesce(foreign_city, pr_city)
            ELSE NULL END
            as city,

            CASE WHEN jurisdiction_id = 2 THEN 'United States'
            ELSE
            coalesce(response.corporation.homeState::VARCHAR, response.mainLocation.foreignDomicileAddress.province::VARCHAR)
            END
            as country_name,
            
            CASE WHEN jurisdiction_id = 2 THEN 'US'
            ELSE
            NULL -- TODO: Add two-letter country codes
            END
            as country_code,
           response.corporation.statusEs as status, 'https://rcp.estado.pr.gov/en/entity-information?c=' || register_index as enlace
    from read_json('recolectas_incrementales/corporation_info/*.json')
    where (jurisdiction = 'Foránea' or jurisdiction = 'Foránea - No Estadounidense') and status = 'FUSIONADA'
    --order by jurisdiction, corp_name
)         
''')
# Jurisdiction ID:
# 1 - Doméstica
# 2 - Foránea
# 3 - Foránea - No Estadounidense

foraneas_fusionadas = con.sql('select * from foraneas_fusionadas').pl()

foraneas_fusionadas.write_excel('outputs/foraneas_fusionadas.xlsx')
print('Wrote all.')