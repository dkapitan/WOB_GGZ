"""Stage DBC Onderhoud codelijsten for WOB_GGZ
"""

import configparser
import xlrd
import csv
import pymssql as sql
import pandas as pd
from wob_ggz import parse_dates, create_missing_values, \
    create_missing_values_tiny


__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'

def main():
    config = configparser.ConfigParser()
    config.read('/opt/projects/wob_ggz/config.ini')
    login = {
        'user': config.get('local_mssql', 'user'),
        'password': config.get('local_mssql', 'password'),
        'server': config.get('local_mssql', 'server'),
        'port': config.get('local_mssql', 'port'),
        'database': config.get('wob_ggz', 'database')}

    cnx = sql.connect(**login)
    cnx.autocommit(True)
    cursor = cnx.cursor()

    dbco_path = config.get('wob_ggz', 'dbco_path')
    staging_path = config.get('wob_ggz', 'staging_path')

    with xlrd.open_workbook(
                    dbco_path +'/20140101 Codelijsten ggz v20140321.xlsx') as wb:
        for sheet_name in wb.sheet_names():
            sh = wb.sheet_by_name(sheet_name)
            with open(dbco_path + '/' + sheet_name + '.csv', 'w') as f:
                c = csv.writer(f, delimiter=';')
                for r in range(sh.nrows):
                    c.writerow(sh.row_values(r))


    ################################
    # read and parse cl_circuit.csv
    ################################
    data_file = dbco_path + '/cl_circuit.csv'
    data = pd.read_csv(data_file, sep=';', dtype=str, encoding='utf8')
    data.insert(0, 'cct_id', range(1, len(data)+1,1))
    data.columns = [x.replace('cl_', 'cct_') for x in data.columns]

    # drop duplicates, take most recent value
    data.drop_duplicates(cols=['cct_circuit_code'], take_last=True, inplace=True)

    # drop date columns, DIM.CIRCUIT treated as non-changing dimension
    data = data.drop(['cct_circuit_begindatum', 'cct_circuit_einddatum'], axis=1)

    # add records for missing values
    missing_values = create_missing_values_tiny(login['database'], 'DIM', 'CIRCUIT', cursor)
    data = pd.concat([data, missing_values],ignore_index=True)
    data.sort(columns=['cct_id'], inplace=True)
    data.to_csv(staging_path + '/DIM.CIRCUIT.csv', sep=';',
                header=True, index = False, encoding='cp1252',
                quoting=None, na_rep='')


    ################################
    # read and parse cl_diagnose.csv
    ################################
    data_file = dbco_path + '/cl_diagnose.csv'
    data = pd.read_csv(data_file, sep=';', dtype=str, encoding='utf8')
    data.insert(0, 'dia_id', range(1, len(data)+1,1))
    data.columns = [x.replace('cl_', 'dia_') for x in data.columns]

    # drop duplicates, take most recent value
    data.drop_duplicates(cols=['dia_diagnose_code'], take_last=True, inplace=True)

    # drop date columns, DIM.DIAGNOSE treated as non-changing dimension
    data = data.drop(['dia_diagnose_begindatum', 'dia_diagnose_einddatum'], axis=1)

    # diagnose_code to uppercase
    data['dia_diagnose_code'] = data['dia_diagnose_code'].apply(lambda x: x.upper())
    data['dia_diagnose_groepcode'] = data['dia_diagnose_groepcode'].apply(lambda x: str(x).upper())

    # add records for missing values
    missing_values = create_missing_values(login['database'], 'DIM', 'DIAGNOSE', cursor)
    data = pd.concat([data, missing_values],ignore_index=True)
    data.sort(columns=['dia_id'], inplace=True)
    data.to_csv(staging_path + '/DIM.DIAGNOSE.csv', sep=';',
                header=True, index = False, encoding='cp1252',
                quoting=None, na_rep='')


    #####################################
    # read and parse cl_prestatiecode.csv
    #####################################
    data_file = dbco_path + '/cl_prestatiecode_ggz.csv'
    data = pd.read_csv(data_file, sep=';', dtype=str, encoding='utf8')
    data.insert(0, 'psc_id', range(1, len(data)+1,1))
    data.columns = [x.replace('cl_', 'psc_') for x in data.columns]

    # drop duplicates, take most recent value
    data.drop_duplicates(cols=['psc_dbc_prestatiecode', 'psc_declaratiecode'],
                         take_last=True, inplace=True)

    # drop date columns, DIM.PRESTATIE treated as non-changing dimension
    data = data.drop(['psc_prestatiecode_begindatum',
                      'psc_prestatiecode_einddatum'], axis=1)

    missing_values = create_missing_values(login['database'], 'DIM', 'PRESTATIECODE', cursor)
    data = pd.concat([data, missing_values],ignore_index=True)
    data.sort(columns=['psc_id'], inplace=True)
    data.to_csv(staging_path + '/DIM.PRESTATIECODE.csv', sep=';',
                header=True, index = False, encoding='cp1252',
                quoting=None, na_rep='')


    #####################################
    # read and parse cl_productgroep.csv
    #####################################
    data_file = dbco_path + '/cl_productgroep_ggz.csv'
    data = pd.read_csv(data_file, sep=';', dtype=str, encoding='utf8')
    data.insert(0, 'prg_id', range(1, len(data)+1,1))
    data.columns = [x.replace('cl_', 'prg_') for x in data.columns]

    # drop duplicates, take most recent value
    data.drop_duplicates(cols=['prg_productgroep_code'],
                         take_last=True, inplace=True)

    # drop date columns, DIM.PRODUCTGROEP treated as non-changing dimension
    data = data.drop(['prg_productgroep_begindatum',
                      'prg_productgroep_einddatum'], axis=1)

    missing_values = create_missing_values_tiny(
        login['database'], 'DIM', 'PRODUCTGROEP', cursor)
    data = pd.concat([data, missing_values],ignore_index=True)
    data.sort(columns=['prg_id'], inplace=True)
    data.to_csv(staging_path + '/DIM.PRODUCTGROEP.csv', sep=';',
                header=True, index = False, encoding='cp1252',
                quoting=None, na_rep='')


    #####################################
    # read and parse cl_redensluiten.csv
    #####################################
    data_file = dbco_path + '/cl_redensluiten.csv'
    data = pd.read_csv(data_file, sep=';', dtype=str, encoding='utf8')
    data.insert(0, 'afs_id', range(1, len(data)+1,1))
    data.columns = [x.replace('cl_', 'afs_') for x in data.columns]
    data.columns = [x.replace('redensluiten_', 'afsluitreden_')
                    for x in data.columns]

    # drop duplicates, take most recent value
    data.drop_duplicates(cols=['afs_afsluitreden_code'],
                         take_last=True, inplace=True)

    # drop date columns, DIM.AFSLUITREDEN treated as non-changing dimension
    data = data.drop(['afs_afsluitreden_begindatum',
                      'afs_afsluitreden_einddatum'], axis=1)

    missing_values = create_missing_values_tiny(
        login['database'], 'DIM', 'AFSLUITREDEN', cursor)
    data = pd.concat([data, missing_values],ignore_index=True)
    data.sort(columns=['afs_id'], inplace=True)
    data.to_csv(staging_path + '/DIM.AFSLUITREDEN.csv', sep=';',
                header=True, index = False, encoding='cp1252',
                quoting=None, na_rep='')


    #####################################
    # read and parse cl_zorgtype.csv
    #####################################
    data_file = dbco_path + '/cl_zorgtype.csv'
    data = pd.read_csv(data_file, sep=';', dtype=str, encoding='utf8')
    data.insert(0, 'zgt_id', range(1, len(data)+1,1))
    data.columns = [x.replace('cl_', 'zgt_') for x in data.columns]

    # drop duplicates, take most recent value
    data.drop_duplicates(cols=['zgt_zorgtype_code'],
                         take_last=True, inplace=True)

    # drop date columns, DIM.ZORGTYPE treated as non-changing dimension
    data = data.drop(['zgt_zorgtype_begindatum',
                      'zgt_zorgtype_einddatum'], axis=1)

    missing_values = create_missing_values(
        login['database'], 'DIM', 'ZORGTYPE', cursor)
    data = pd.concat([data, missing_values],ignore_index=True)
    data.sort(columns=['zgt_id'], inplace=True)
    data.to_csv(staging_path + '/DIM.ZORGTYPE.csv', sep=';',
                header=True, index = False, encoding='cp1252',
                quoting=None, na_rep='')


if __name__ == '__main__':
    main()