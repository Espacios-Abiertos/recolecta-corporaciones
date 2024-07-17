import os
import time
import requests
import duckdb
import polars as pl
import json
import multiprocessing

from utils_database import read_query



class Worker(multiprocessing.Process):

    def __init__(self, job_queue, num_corporations_to_recolectar):
        super().__init__()
        self.__job_queue = job_queue
        self.num_corporations_to_recolectar = num_corporations_to_recolectar

    def run(self):
        incremental_dir = 'recolectas_incrementales/related_entities'
        request_url_prefix = 'https://rceapi.estado.pr.gov/api/corporation/relatedentities/'
        while True:
            corp = self.__job_queue.get()
            if corp is None:
                break

            # Actual code
            print(f"({corp['i']+1}/{self.num_corporations_to_recolectar}) {corp['registration_index']} {corp.get('corp_name', '<N/A>')}")
            fpath = os.path.join(incremental_dir, corp['registration_index'] + '.json')

            if os.path.isfile(fpath):
                print('Ya existe. Saltando.')
                continue
            
            url = request_url_prefix + corp['registration_index']

            downloaded = False
            download_attempts = 0
            while not downloaded:
                if download_attempts == 10:
                    print('Too many download attempts. Erroring out.')
                    raise Exception('Too many download attempts')
                elif download_attempts > 0:
                    print('Retrying...')
                download_attempts += 1

                r = requests.get(url)
                try:
                    r.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    print(f'Error: {e}', e.response.status_code)
                    if e.response.status_code == 429:
                        print('Sent too many requests. Waiting briefly...')
                        time.sleep(10)
                        continue
                    elif e.response.status_code == 520:
                        print('Error 520. Waiting briefly...')
                        time.sleep(10)
                        continue
                    else:
                        raise e
                
                downloaded = True

            data = r.json()
            with open(fpath, 'w') as f:
                json.dump(data, f)


if __name__ == '__main__':

    # con = duckdb.connect(':memory:') # maybe change later?
    # con.execute(read_query('utils'))
    # con.execute(read_query('attach_db_readonly'))
    # for q in read_query('apareo_decretos_corporaciones').split(';'):
    #     con.execute(q)

    # con.execute(
    # '''
    # create or replace table grantee_matches as (
    #     from read_csv_auto('outputs/grantee_matches.csv')
    # )
    # '''
    # )
    # con.execute(
    # '''
    # create or replace table corporations_to_recolectar as (
    # -- recolectar solo aquellas que esten funcionando todavía
    # with valid_status_matches as (
    #     -- from grantee_matches -- solo los matches del listado de grantees del DDEC
    #     from corporaciones -- the whole list de corps
    #     where list_contains(['ACTIVA','FUSIONADA','CONVERSION','ENMENDADA'], status_es)
    # )

    # select distinct on(registration_index) registration_index, corp_name, status_es
    # from valid_status_matches
    # order by corp_name
    # )
    # '''
    # )

    # print('corporations_to_recolectar:')
    # print(con.sql('from corporations_to_recolectar'))
    # corporations_to_recolectar_rel = con.sql('from corporations_to_recolectar')

    foraneas_fusionadas = (
        pl.read_excel('outputs/foraneas_fusionadas.xlsx')
        .select(pl.col('register_index').alias('registration_index'), 'corp_name', 'status')
    )

    # corporations_to_recolectar = [
    #     {'i': i, 'registration_index': row[0], 'corp_name': row[1], 'status_es': row[2]}
    #     for (i,row) in enumerate(corporations_to_recolectar_rel.fetchall())
    # ]
    # corporations_to_recolectar = [
    #     c for c in corporations_to_recolectar
    #     if c['registration_index'] not in [
    #         '19788-121'
    #     ]
    # ]
    corporations_to_recolectar = list(foraneas_fusionadas.iter_rows(named=True))
    

    # con.close()
    num_corporations_to_recolectar = len(corporations_to_recolectar)
    # for (i,corp) in enumerate(corporations_to_recolectar):
    #     print(f"({i+1}/{num_corporations_to_recolectar}) {corp['registration_index']} {corp['corp_name']}")
    #     fpath = os.path.join(incremental_dir, corp['registration_index'] + '.json')

    #     if os.path.isfile(fpath):
    #         print('Ya existe. Saltando.')
    #         continue

    #     url = request_url_prefix + corp['registration_index']

    #     downloaded = False
    #     download_attempts = 0
    #     while not downloaded:
    #         if download_attempts == 10:
    #             print('Too many download attempts. Erroring out.')
    #             raise Exception('Too many download attempts')
    #         elif download_attempts > 0:
    #             print('Retrying...')
    #         download_attempts += 1

    #         r = requests.get(url)
    #         try:
    #             r.raise_for_status()
    #         except requests.exceptions.HTTPError as e:
    #             print(f'Error: {e}', e.response.status_code)
    #             if e.response.status_code == 429:
    #                 print('Sent too many requests. Waiting briefly...')
    #                 time.sleep(5)
    #                 continue
    #             else:
    #                 raise e
            
    #         downloaded = True

    #     data = r.json()
    #     with open(fpath, 'w') as f:
    #         json.dump(data, f)

    #     if (i + 1) % 25 == 0:
    #         print('Sleeping briefly...')
    #         time.sleep(5)

    #     # break
    

    workers = []
    job_queue = multiprocessing.Queue()

    num_workers = 1
    for i in range(num_workers):
        p = Worker(job_queue, num_corporations_to_recolectar)
        workers.append(p)
        p.start()

    for (i, corp) in enumerate(corporations_to_recolectar):
        corp['i'] = i
        job_queue.put(corp)

    # To signal the workers to stop after done
    for i in range(num_workers):
        job_queue.put(None)

    for p in workers:
        p.join()