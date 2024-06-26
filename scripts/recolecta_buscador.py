import requests
import datetime
import pytz

buscador_url = 'https://rceapi.estado.pr.gov/api/corporation/search'

def get_buscador_payload():
    payload = {
        'advanceSearch': None,
        'cancellationMode': False,
        'isWorkFlowSearch': False,
        'limit': 1000, # limite de 1000
        'matchType': 4, # what is this?
        'method': None,
        'onlyActive': False,

        'corpName': None, # also a parameter

        # Default case: no busca por número de registro
        'comparisonType': 1, # varia
        'registryNumber': None, # Este es el parameter
    }

    return payload

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

# Post request
payload = get_buscador_payload()
comparison = get_comparison('Mayor que', 0)
payload.update(comparison)

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




# r = requests.post(buscador_url, json=payload)
# print(r.status_code)
# print(r.json())