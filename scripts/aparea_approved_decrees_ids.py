# Para aparear los nombres de los approved decrees
# con los registration_index (los IDs) de
# las entidades del Registro de Corporaciones

import os
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
create table if not exists grantee_search_matches (
            grantee VARCHAR NOT NULL,
            registration_index VARCHAR NOT NULL,
            corp_name VARCHAR NOT NULL,
            status_id INTEGER, status_es VARCHAR, score DOUBLE,

            primary key (grantee, registration_index)
);
''')

grantees_that_need_search = con.sql('''
select grantee from (from exact_matches where match_type is null)
anti join grantee_search_matches
using (grantee)    
''').pl().to_series().to_list()
print('Num grantees needing search:', len(grantees_that_need_search))
print()

for i, grantee in enumerate(grantees_that_need_search):
    print(f'{i+1}/{len(grantees_that_need_search)}:', grantee)
    grantee_escaped = grantee.replace("'","''")
    r1 = con.sql(f"execute corporaciones_fts_query('{grantee_escaped}', 1)")
    # print('r1:', r1, sep='\n')
    grantee_search_result = con.sql(f'''select '{grantee_escaped}' as grantee, * from r1''')
    # print('grantee_search_result:', grantee_search_result, sep='\n')
    print(f'{i+1}/{len(grantees_that_need_search)}:', grantee, '-->', grantee_search_result.fetchone()[2])

    # Change to insert
    con.execute('''
    insert into grantee_search_matches by name (
        from grantee_search_result
    ) on conflict do nothing
    ''')
    
    
    # print('grantee_search_matches:', grantee_search_matches, sep='\n')
    # print()

    # if i+1 >= 20:
    #     break

# maybe not necessary yet
# con.execute("alter table grantee_search_matches add column match_type varchar default 'search'")

grantee_search_matches = con.sql('from grantee_search_matches')
print('grantee_search_matches:', grantee_search_matches, sep='\n')

con.execute('drop table if exists grantee_candidate_matches') # COMMENT OUT LATER
con.execute('''
create table if not exists grantee_candidate_matches (
            -- same as grantee_search_matches
            grantee VARCHAR NOT NULL,
            registration_index VARCHAR NOT NULL,
            corp_name VARCHAR NOT NULL,
            status_id INTEGER,
            status_es VARCHAR, score DOUBLE,

            -- just adding these columns to the grantee_search_matches schema
            aprobado VARCHAR, rechazado VARCHAR,
            evaluado BOOLEAN generated always as (
             (aprobado is not null) or (rechazado is not null)
             ) virtual,
            
            primary key (grantee, registration_index)
);
''')
# con.execute('''
# update grantee_candidate_matches set evaluado =   (
#             (aprobado is not null) or (rechazado is not null)
#             );       
# ''')

print('grantee_candidate_matches (before)')
print(con.sql('from grantee_candidate_matches'))

# Add new candidates as needed
# while keeping evaluados safe

# con.execute('delete from grantee_candidate_matches where evaluado = false')

# check if os isfile

# COMMENT LATER
# if os.path.isfile('manual_editing/grantee_candidate_matches.csv'):
#     os.remove('manual_editing/grantee_candidate_matches.csv')

if os.path.isfile('manual_editing/grantee_candidate_matches.csv'):
    con.execute(
    '''
    create or replace table grantee_candidate_matches_editing (
        registration_index VARCHAR NOT NULL,
        grantee VARCHAR NOT NULL,
        corp_name VARCHAR NOT NULL,
        aprobado VARCHAR, rechazado VARCHAR
    )
    '''
    )
    con.execute(
    '''
    insert into grantee_candidate_matches_editing by name (
        select * from read_csv_auto('manual_editing/grantee_candidate_matches.csv')
    )
    '''
    )

    # Insert evaluados nuevos
    con.execute(
    '''
    insert into grantee_candidate_matches by name (
        from grantee_candidate_matches_editing
        --anti join grantee_candidate_matches
        --using (grantee, corp_name)
        --where (grantee_candidate_matches_editing.aprobado is not null)
        --    or (grantee_candidate_matches_editing.rechazado is not null)
    )
    on conflict do update set aprobado = excluded.aprobado, rechazado = excluded.rechazado
    '''
    )

    # Update evaluados nuevos
    con.execute('''
    update grantee_candidate_matches
        set status_id = grantee_search_matches.status_id,
            status_es = grantee_search_matches.status_es,
            score = grantee_search_matches.score
    from grantee_search_matches
    where grantee_candidate_matches.grantee = grantee_search_matches.grantee
        and grantee_candidate_matches.registration_index = grantee_search_matches.registration_index
        and grantee_candidate_matches.corp_name = grantee_search_matches.corp_name
    ''')

print('grantee_candidate_matches (after csv insert)')
print(con.sql('from grantee_candidate_matches'))


# print('Gonna insert these rows:')
# print(con.sql(
# '''
# with evaluated_candidate_matches as (
#         from grantee_candidate_matches
#         where evaluado = true
#     )

#     from grantee_search_matches
#     anti join evaluated_candidate_matches
#     using (grantee, corp_name)
# '''
# ))
con.execute('''
insert into grantee_candidate_matches by name (
    with evaluated_candidate_matches as (
        from grantee_candidate_matches
        where evaluado = true
    )

    from grantee_search_matches
    anti join evaluated_candidate_matches
    using (grantee, corp_name)    
) on conflict do nothing     
''')
# con.execute(
# '''
# create or replace table grantee_candidate_matches as (
# with grantee_candidate_matches_evaluado as (
#     from grantee_candidate_matches
#     where evaluado = true
# ),

# grantee_candidate_matches_no_evaluado as (
#     select grantee_search_matches.*, 
#         aprobado, rechazado,
#         coalesce(grantee_candidate_matches.evaluado, false) as new_evaluado
#     from grantee_search_matches
#     left join grantee_candidate_matches
#     using (grantee, registration_index)
#     where new_evaluado = false
# )

# select * exclude (new_evaluado), new_evaluado as evaluado
# from grantee_candidate_matches_no_evaluado
# union all by name
# from grantee_candidate_matches_evaluado

# );
# ''')

con.execute('''
create or replace view grantee_matches as (
with grantee_candidate_matches_aprobados as (
    select *, 'search_aprobado' as match_type
    from grantee_candidate_matches
    where aprobado is not null
)
            
select grantee, registration_index, corp_name, status_es, match_type from exact_matches where match_type = 'exact'
union all by name
select grantee, registration_index, corp_name, status_es, match_type from grantee_candidate_matches_aprobados
)
''')

print('grantee_candidate_matches (after)')
print(con.sql('from grantee_candidate_matches'))
print('Para ver los más repetidos (usually more means more generic trash matches):')
print(con.sql('select corp_name, count(*) as thecount from grantee_candidate_matches group by all order by thecount desc'))
print('Distribución de estatus grantee_candidate_matches:')
print(con.sql('select status_es, count(*) as thecount from grantee_candidate_matches group by all order by thecount desc'))
print('Distribución de estatus grantee_matches:')
print(con.sql('select status_es, count(*) as thecount from grantee_matches group by all order by thecount desc'))

grantee_matches = con.sql('from grantee_matches').pl()
grantee_matches.write_csv('outputs/grantee_matches.csv')

grantee_candidate_matches_editing = con.sql(
'''
select registration_index, grantee, corp_name, aprobado, rechazado
from grantee_candidate_matches
order by evaluado, score desc
''').pl()
grantee_candidate_matches_editing.write_csv('manual_editing/grantee_candidate_matches.csv')

print(con.sql('select evaluado, count(*) from grantee_candidate_matches group by evaluado'))

con.close()