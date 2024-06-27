-- DuckDB Macros

create or replace macro preprocess_text(string) as
    nfc_normalize(string)
    .lower()
    .ltrim().rtrim()
    .replace('.','')
    .replace(',','')
    .strip_accents();