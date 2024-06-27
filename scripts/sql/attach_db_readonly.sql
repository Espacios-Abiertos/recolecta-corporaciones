-- For attaching the sqlite database
-- into the DuckDB client in read-only mode

install sqlite;
load sqlite;

attach if not exists 'recolecta_corporaciones.db' as recolecta_corporaciones (type sqlite, read_only);