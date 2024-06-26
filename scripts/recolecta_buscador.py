import requests
import datetime
import pytz
import json
from utils_database import scaffold_database, insert_recolecta_buscador_request, \
    insert_recolecta_buscador_records

db_path = 'recolecta_corporaciones.db'
buscador_url = 'https://rceapi.estado.pr.gov/api/corporation/search'

def get_buscador_payload(limit:int = 1000):

    if (limit < 1) or (limit > 1000):
        raise ValueError('El límite debe estar entre 1 y 1000')
    
    payload = {
        'advanceSearch': None,
        'cancellationMode': False,
        'isWorkFlowSearch': False,
        'limit': limit,
        'method': None,
        'onlyActive': False,

        # Default case: no busca por nombre de corporación
        'matchType': 4, # varia
        'corpName': None, # also a parameter

        # Default case: no busca por número de registro
        'comparisonType': 1, # varia
        'registryNumber': None, # Este es el parameter
    }

    return payload

# match types:
# 1 - Comenzando con
# 2 - Pareo exacto
# 3 - Cualquier palabra
# 4 - Todas las palabras
def get_match(match_type_description: str, corp_name: str):
    match = {'corpName': corp_name}

    if match_type_description == 'Comenzando con':
        match['matchType'] = 1
    elif match_type_description == 'Pareo exacto':
        match['matchType'] = 2
    elif match_type_description == 'Cualquier palabra':
        match['matchType'] = 3
    elif match_type_description == 'Todas las palabras':
        match['matchType'] = 4
    else:
        raise ValueError(f'No se reconoce el tipo de búsqueda: "{match_type_description}"')
    
    return match


# comparison types:
# 1 - Es igual a
# 2 - Mayor que
# 3 - Menor que
def get_comparison(comparison_type_description: str, registry_number: int):
    comparison = {'registryNumber': registry_number}

    if comparison_type_description == 'Es igual a':
        comparison['comparisonType'] = 1
    elif comparison_type_description == 'Mayor que':
        comparison['comparisonType'] = 2
    elif comparison_type_description == 'Menor que':
        comparison['comparisonType'] = 3
    elif comparison_type_description == 'Mayor o igual que':
        comparison['comparisonType'] = 2
        comparison['registryNumber'] = registry_number - 1
    elif comparison_type_description == 'Menor o igual que':
        comparison['comparisonType'] = 3
        comparison['registryNumber'] = registry_number + 1
    else:
        raise ValueError(f'No se reconoce el tipo de comparación: "{comparison_type_description}"')
    
    return comparison

def get_puerto_rico_timestamp(verbose=False):
    system_now = datetime.datetime.now().astimezone()
    desired_timezone = pytz.timezone('America/Puerto_Rico')
    desired_now = system_now.astimezone(desired_timezone)

    timestamp = (
        desired_now
        .replace(microsecond=0).isoformat()
    )

    if verbose:
        print('System now:', system_now)
        print('Desired now:', desired_now)
        print('Timestamp:', timestamp)

    return timestamp


# Ready database
scaffold_database(db_path)


# Post request
payload = get_buscador_payload()
comparison = get_comparison('Mayor que', 0)
payload.update(comparison)

timestamp = get_puerto_rico_timestamp()
print('Timestamp:', timestamp)
r = requests.post(buscador_url, json=payload)
print(r.status_code)
r.raise_for_status()

request_id = insert_recolecta_buscador_request(db_path, timestamp)
print('Request ID:', request_id)



# Save to file
json_response = r.json()
with open('buscador_response.json', 'w') as f:
    json.dump(json_response, f)

records = json_response['response']['records']
insert_recolecta_buscador_records(db_path, request_id, records)
# print(r.json())