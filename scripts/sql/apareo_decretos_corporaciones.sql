-- Scaffold los codigos de apareo de decretos y corporaciones
-- Usa DuckDB

install fts;
load fts;

create or replace table decretos as (
    from 'outputs/decrees.parquet'
);

create or replace table corporaciones as (
    select distinct registration_index, corp_name, status_id, status_es
    from recolecta_corporaciones.recolecta_buscador__records
    order by record_id
);

-- Prepara el indice de BM25
pragma create_fts_index(
    'corporaciones', 'registration_index',
    'corp_name', overwrite = 1
);

prepare corporaciones_fts_query as (
    with scored_corps as (
        select *, fts_main_corporaciones.match_bm25(registration_index, ?) as score
        from corporaciones
    )
    select *
    from scored_corps
    where score is not null
    order by status_id = 1 desc, score desc
    limit 10
);