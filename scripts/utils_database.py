import sqlite3

def scaffold_database(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS recolecta_buscador__requests (
            request_id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_sent_timestamp TEXT NOT NULL
        ) STRICT;
    ''')

    {
        "businessEntityId": 702452,
        "registrationNumber": 1,
        "registrationIndex": "1-811",
        "corpName": "CENTRAL RADIOLOGY SERVICES LLP",
        "classEs": "Soc. Resp. Limitada",
        "classEn": "L.L.P.",
        "profitTypeEs": "Con Fines",
        "profitTypeEn": "For Profit",
        "statusId": 9,
        "statusEs": "EXPIRADA",
        "statusEn": "EXPIRED"
      }
    
    c.execute('''
        CREATE TABLE IF NOT EXISTS recolecta_buscador__records (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id INTEGER NOT NULL,
            business_entity_id INTEGER NOT NULL,
            registration_number INTEGER NOT NULL,
            registration_index TEXT NOT NULL,
            corp_name TEXT NOT NULL,
            class_es TEXT NOT NULL,
            class_en TEXT NOT NULL,
            profit_type_es TEXT NOT NULL,
            profit_type_en TEXT NOT NULL,
            status_id INTEGER NOT NULL,
            status_es TEXT NOT NULL,
            status_en TEXT NOT NULL,
            FOREIGN KEY (request_id) REFERENCES recolecta_buscador__requests(request_id)
        ) STRICT;          
    ''')

    conn.commit()
    conn.close()

# recolecta_buscador
def insert_recolecta_buscador_request(db_path, timestamp):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''
        INSERT INTO recolecta_buscador__requests (request_sent_timestamp)
        VALUES (?);
    ''', (timestamp,))
    generated_request_id = c.lastrowid

    conn.commit()
    conn.close()

    return generated_request_id

recolecta_buscador_record_variable_name_mapping = {
    'businessEntityId': 'business_entity_id',
    'registrationNumber': 'registration_number',
    'registrationIndex': 'registration_index',
    'corpName': 'corp_name',
    'classEs': 'class_es',
    'classEn': 'class_en',
    'profitTypeEs': 'profit_type_es',
    'profitTypeEn': 'profit_type_en',
    'statusId': 'status_id',
    'statusEs': 'status_es',
    'statusEn': 'status_en'
}
recolecta_buscador_record_variable_name_inverse_mapping = {
    value: key
    for key, value in recolecta_buscador_record_variable_name_mapping.items()
}
def insert_recolecta_buscador_records(db_path, request_id, records):
    columns = tuple(recolecta_buscador_record_variable_name_mapping.values())
    original_columns = tuple(recolecta_buscador_record_variable_name_inverse_mapping[k] for k in columns)
    # print('Columns:', columns)
    # print('Columns string:', ', '.join(columns))
    # print('Original columns:', original_columns)

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    data = [
        (request_id, *tuple(r[key] for key in original_columns))
        for r in records
    ]

    c.executemany(f'''
        INSERT INTO recolecta_buscador__records (request_id, {', '.join(columns)})
        VALUES ({', '.join(['?'] * (len(columns) + 1))});
    ''', data)

    conn.commit()
    conn.close()

def get_recolecta_buscador_max_registration_number(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('''
        SELECT MAX(registration_number) FROM recolecta_buscador__records;
    ''')
    max_registration_number = c.fetchone()[0]
    conn.close()

    return max_registration_number