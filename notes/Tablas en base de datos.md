Este documento sirve para documentar sobre las tablas presentes en la base de datos sqlite de `recolecta_corporaciones.db` y las implicaciones de estas.
## recolecta_buscador__records

|     column_name     | column_type | null | key | default | extra |
|---------------------|-------------|------|-----|---------|-------|
| record_id           | BIGINT      | YES  |     |         |       |
| request_id          | BIGINT      | YES  |     |         |       |
| business_entity_id  | BIGINT      | YES  |     |         |       |
| registration_number | BIGINT      | YES  |     |         |       |
| registration_index  | VARCHAR     | YES  |     |         |       |
| corp_name           | VARCHAR     | YES  |     |         |       |
| class_es            | VARCHAR     | YES  |     |         |       |
| class_en            | VARCHAR     | YES  |     |         |       |
| profit_type_es      | VARCHAR     | YES  |     |         |       |
| profit_type_en      | VARCHAR     | YES  |     |         |       |
| status_id           | BIGINT      | YES  |     |         |       |
| status_es           | VARCHAR     | YES  |     |         |       |
| status_en           | VARCHAR     | YES  |     |         |       |
"record_id" es el primary key. "request_id" es el foreign key apuntando a cual request dio ese record. Dejandonos llevar por los enlaces de la página del registro de corporaciones, deberiamos usar el registration index para segmentar IDs.

```sql
select distinct registration_index, corp_name, status_id, status_es
from recolecta_corporaciones.recolecta_buscador__records
order by record_id
```

Nota: Por ahora, solo se recolectaron corporaciones con registration_number mayor o igual a cero. De ser necesario recolectar las demás, se hará tal.

