# Para aparear los nombres de los approved decrees
# con los registration_index (los IDs) de
# las entidades del Registro de Corporaciones

import duckdb
import polars as pl
from utils_database import read_query

# Importar los decretos aprobados
decrees = pl.read_parquet('outputs/decrees.parquet')

# Importar los nombres de las compañías en el Registro de Corporaciones
con = duckdb.connect('databases/aparea_approved_decrees_ids.db')

con.execute(read_query('utils'))
con.execute(read_query('attach_db_readonly'))
for q in read_query('apareo_decretos_corporaciones').split(';'):
    con.execute(q)

grantees_with_exact_match = con.sql("from exact_matches where match_type = 'exact'")
print('grantees_with_exact_match', grantees_with_exact_match, sep='\n')

grantees_without_exact_match = con.sql('select grantee from exact_matches where match_type is null').pl().to_series().to_list()
print('Num grantees without exact matches:', len(grantees_without_exact_match))
print()

# Get search matches for grantees without exact matches
con.execute('''
create or replace table grantee_search_matches (
            grantee VARCHAR, registration_index VARCHAR,
            corp_name VARCHAR, status_id INTEGER,
            status_es VARCHAR, score DOUBLE
);
''')
for i, grantee in enumerate(grantees_without_exact_match):
    # print(f'{i+1}/{len(grantees_without_exact_match)}:', grantee)
    r1 = con.sql(f"execute corporaciones_fts_query('{grantee}', 1)")
    # print('r1:', r1, sep='\n')
    grantee_search_result = con.sql(f'''select '{grantee}' as grantee, * from r1''')
    # print('grantee_search_result:', grantee_search_result, sep='\n')
    print(f'{i+1}/{len(grantees_without_exact_match)}:', grantee, '-->', grantee_search_result.fetchone()[2])

    con.execute('''
    create or replace table grantee_search_matches as (
        from grantee_search_matches
        union all by name
        from grantee_search_result
    )
    ''')
    
    
    # print('grantee_search_matches:', grantee_search_matches, sep='\n')
    # print()

    if i >= 10 - 1:
        break

# maybe not necessary yet
# con.execute("alter table grantee_search_matches add column match_type varchar default 'search'")

grantee_search_matches = con.sql('from grantee_search_matches')
print('grantee_search_matches:', grantee_search_matches, sep='\n')

con.execute('''
create table if not exists grantee_candidate_matches (
            -- same as grantee_search_matches
            grantee VARCHAR, registration_index VARCHAR,
            corp_name VARCHAR, status_id INTEGER,
            status_es VARCHAR, score DOUBLE,

            -- just adding these columns to the grantee_search_matches schema
            aprobado VARCHAR, rechazado VARCHAR,
            evaluado BOOLEAN
);
''')
con.execute('''
update grantee_candidate_matches set evaluado =   (
            (aprobado is not null) or (rechazado is not null)
            );       
''')
# con.execute('''
# update          
# ''')

print('grantee_candidate_matches (before)')
print(con.sql('from grantee_candidate_matches'))

# Add new candidates as needed
# while keeping evaluados safe
con.execute(
'''
create or replace table grantee_candidate_matches as (
with grantee_candidate_matches_evaluado as (
    from grantee_candidate_matches
    where evaluado = true
),

grantee_candidate_matches_no_evaluado as (
    select grantee_search_matches.*, 
        aprobado, rechazado,
        case when grantee_candidate_matches.evaluado is null
              then false
              else grantee_candidate_matches.evaluado
              end as new_evaluado,
    from grantee_search_matches
    left join grantee_candidate_matches
    using (grantee, registration_index)
    where new_evaluado = false
)

select * exclude (new_evaluado), new_evaluado as evaluado
from grantee_candidate_matches_no_evaluado
union all by name
from grantee_candidate_matches_evaluado

);
''')

print('grantee_candidate_matches (after)')
print(con.sql('from grantee_candidate_matches'))

con.close()