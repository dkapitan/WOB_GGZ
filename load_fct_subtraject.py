#!/usr/bin/env python
""" Script to load fact-subtraject from wob_zz

ETL of WOB GGZ dataset based on pygrametl framework.

NB:     python table names are DIM_xxx, FCT_yyy;
        MS SQL schema.table names are DIM.xxx, FCT.yyy

NNB:    tempdest is stored in /var/folders/ ...
        This path should be available to MS SQL Server,
        e.g. via sharing in Parallels


"""

import bz2
import csv
import configparser
import pymssql as sql
import time
import os
import pygrametl as etl
from pygrametl.tables import CachedDimension, BulkDimension, \
    SlowlyChangingDimension, BulkFactTable
from wob_ggz import *

# import cProfile, pstats, StringIO

__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'


def mssql_bulkloader(tablename, attributes, fieldsep, rowsep, nullsubst, tempdest):
    """Bulkloader using MS SQL Server bulk insert.

    This works for the following setup:
    - Python 3 (anaconda) running on OSX
    - SQL Server 2014 running on Windows 7 Ultimate running on Parallels 9
    - pymssql / FreeTDS as python DB API
    - /var/library directory that has tempdest files is shared via Parallels

    NB:
    - rowterminator = '0x0a' due to differences Windows vs. UNIX
    - ms sql want full datetime, e.g. 2007-01-10 00:00:00, for dates
        that are loaded with bulk insert
    - ms sql can't deal with text delimiters --> do without,
        check no delimiters in fields
    - open statements with " or ''' and use single quotes
        for strings in sql statements!!
    """
    global cur
    win_temp = '\\\\psf' + tempdest.replace('/','\\')
    stmt = ('''bulk insert {} from '{}'
               with (firstrow=1,
               fieldterminator='\\t',
               rowterminator='0x0a',
               codepage='1252')
             '''.format(tablename, win_temp))
    print("    sql> " + stmt)
    cur.execute(stmt)
    print("    number of rows affected: {}".format(cur.rowcount))


# setup connection to database
config = configparser.ConfigParser()
config.read('/opt/projects/wob_ggz/config.ini')

login = {
    'user': config.get('local_mssql', 'user'),
    'password': config.get('local_mssql', 'password'),
    'server': config.get('local_mssql', 'server'),
    'port': config.get('local_mssql', 'port'),
    'database': config.get('wob_ggz', 'database')
    }

cnx = sql.connect(**login)
cur = cnx.cursor()
connection = etl.ConnectionWrapper(cnx)
connection.setasdefault()


# define dimension object for ETL
# Note that:
# - pygrametl object table names are DIM_xxx, FCT_yyy
# - MS SQL schema.table names are DIM.xxx, FCT.yyy
DIM_AFSLUITREDEN = CachedDimension(
name='DIM.AFSLUITREDEN',
key='afs_id',
attributes=['afs_afsluitreden_code'],
size=0,
prefill=True
)

DIM_CIRCUIT = CachedDimension (
name='DIM.CIRCUIT',
key='cct_id',
attributes=['cct_circuit_code'],
size=0,
prefill=True
)

DIM_DAG = CachedDimension(
    name='DIM.DAG',
    key='dag_id',
    attributes=['dag_datum'],
    size=0,
    prefill=True
)

DIM_DIAGNOSE = CachedDimension(
    name='DIM.DIAGNOSE',
    key='dia_id',
    attributes=['dia_diagnose_code'],
    size=0,
    prefill=True
)

DIM_LAND = CachedDimension(
    name='DIM.LAND',
    key='lnd_id',
    attributes=['lnd_land_code'],
    size=0,
    prefill=True
)

DIM_PRESTATIECODE = CachedDimension(
    name='DIM.PRESTATIECODE',
    key='psc_id',
    attributes=['psc_dbc_prestatiecode', 'psc_declaratiecode'],
    size=0,
    prefill=True
)

DIM_PRODUCTGROEP = CachedDimension(
    name='DIM.PRODUCTGROEP',
    key='prg_id',
    attributes=['prg_productgroep_code'],
    size=0,
    prefill=True
)

DIM_SUBTRAJECTNUMMER = BulkDimension(
    name='DIM.SUBTRAJECTNUMMER',
    key='stn_id',
    attributes=['stn_subtrajectnummer', 'stn_zorgtrajectnummer'],
    nullsubst='',
    fieldsep='\t',
    rowsep='\r\n',
    usefilename=True,
    bulkloader=mssql_bulkloader
)

DIM_ZORGTYPE = CachedDimension(
    name='DIM.ZORGTYPE',
    key='zgt_id',
    attributes=['zgt_zorgtype_code'],
    size=0,
    prefill=True
)

FCT_SUBTRAJECT = BulkFactTable(
    name='FCT.SUBTRAJECT',

    # NB: order matters for MS SQL bulk insert!
    keyrefs=['stn_id',
             'dag_id_begindatum_zorgtraject',
             'dag_id_einddatum_zorgtraject',
             'dag_id_begindatum_subtraject',
             'dag_id_einddatum_subtraject',
             'dag_id_diagnose',
             'zgt_id',
             'dia_id_primair',
             'prg_id',
             'psc_id',
             'cct_id',
             'afs_id',
             'lnd_id'],
    measures=['geslacht', 'diagnose_trekken_van', 'fct_verkoopprijs',
              'fct_tarief', 'fct_verrekenbedrag'],
    nullsubst='',
    fieldsep='\t',
    rowsep='\r\n',
    usefilename=True,
    bulkloader=mssql_bulkloader
)

def load_subtraject(file, config):
    """Method for loading one subtraject file of WOB ZZ DOT

    Main ETL method for WOB GGZ subtrajecten.
    Requires active pygrametl connection as global.
    """
    global connection
    paths = {'data_path': config.get('wob_ggz', 'data_path'),
             'staging_path': config.get('wob_ggz', 'staging_path')}
    column_names = ['datum_aanmaak',
                 'geslacht', 
                 'landcode', 
                 'zorgtrajectnummer',
                 'begindatum_zorgtraject',
                 'einddatum_zorgtraject',
                 'primaire_diagnose_code',
                 'primaire_diagnose_trekken_van',
                 'primaire_diagnose_datum',
                 'dbc_trajectnummer',
                 'zorgtypecode',
                 'circuitcode',
                 'productgroepcode',
                 'begindatum_dbc_traject',
                 'einddatum_dbc_traject',
                 'dbc_reden_sluiten_code',
                 'verkoopprijs_dbc',
                 'prestatiecode',
                 'declaratiecode',
                 'dbc_tarief',
                 'verrekenbedrag'
    ]

    source_file = bz2.open(paths['data_path'] + '/' + file,
                           mode='rt', encoding='cp1252')
    source = csv.DictReader(source_file, delimiter=';',
                            quotechar='"',
                            fieldnames=column_names)

    starttime = time.localtime()
    start_s = time.time()
    print('{} - Start processing file: {}'.
          format(time.strftime('%H:%M:%S', starttime), file))

    name_mapping = {
        'geslacht': 'geslacht',
        'lnd_land_code': 'landcode',
        'stn_zorgtrajectnummer': 'zorgtrajectnummer',
        'dia_diagnose_code': 'primaire_diagnose_code',
        'diagnose_trekken_van': 'primaire_diagnose_trekken_van',
        'stn_subtrajectnummer': 'dbc_trajectnummer',
        'zgt_zorgtype_code': 'zorgtypecode',
        'cct_circuit_code': 'circuitcode',
        'prg_productgroep_code': 'productgroepcode',
        'afs_afsluitreden_code': 'dbc_reden_sluiten_code',
        'fct_verkoopprijs': 'verkoopprijs_dbc',
        'psc_dbc_prestatiecode': 'prestatiecode',
        'psc_declaratiecode': 'declaratiecode',
        'fct_tarief': 'dbc_tarief',
        'fct_verrekenbedrag': 'verrekenbedrag'
    }

    for row in source:

        # convert datecolumns to appropriate format
        row['begindatum_zorgtraject'] = parse_dates(row['begindatum_zorgtraject'])
        row['einddatum_zorgtraject'] = parse_dates(row['einddatum_zorgtraject'])
        row['begindatum_dbc_traject'] = parse_dates(row['begindatum_dbc_traject'])
        row['einddatum_dbc_traject'] = parse_dates(row['einddatum_dbc_traject'])
        row['primaire_diagnose_datum'] = parse_dates(row['primaire_diagnose_datum'])

        # ensure DBC codes are filled to right length
        row['zorgtypecode'] = parse_codes(row['zorgtypecode'], 3, '_?_')
        row['primaire_diagnose_code'] = etl.getstr(row['primaire_diagnose_code']).upper()
        row['productgroepcode'] = parse_codes(row['productgroepcode'], 6, '_?_')
        row['prestatiecode'] = parse_codes(row['prestatiecode'], 12, '_?_')

        # get tinyint codes
        row['dbc_reden_sluiten_code'] = etl.getint(
            row['dbc_reden_sluiten_code'], default=0)
        row['circuitcode'] = etl.getint(
            row['circuitcode'], default=0)

        # convert geslacht into int conform COD046_NEN / Vektis
        row['geslacht'] = etl.getint(row['geslacht'], default=0)

        # diagnose_trekken_van: 'spatie' = 0, J=1
        row['primaire_diagnose_trekken_van'] = parse_boolean(
            row['primaire_diagnose_trekken_van'], default=0
        )

        # convert money values into decimals
        row['verkoopprijs_dbc'] = parse_money(row['verkoopprijs_dbc'])
        row['dbc_tarief'] = parse_money(row['dbc_tarief'])
        row['verrekenbedrag'] = parse_money(row['verrekenbedrag'])

        # derive dimension_ids
        row['stn_id'] = DIM_SUBTRAJECTNUMMER.ensure(row, name_mapping)
        row['dag_id_begindatum_zorgtraject'] = DIM_DAG.lookup(
            row, {'dag_datum': 'begindatum_zorgtraject'})
        row['dag_id_einddatum_zorgtraject'] = DIM_DAG.lookup(
            row, {'dag_datum': 'einddatum_zorgtraject'})
        row['dag_id_begindatum_subtraject'] = DIM_DAG.lookup(
            row, {'dag_datum': 'begindatum_dbc_traject'})
        row['dag_id_einddatum_subtraject'] = DIM_DAG.lookup(
            row, {'dag_datum': 'einddatum_dbc_traject'})
        row['dag_id_diagnose'] = DIM_DAG.lookup(
            row, {'dag_datum': 'primaire_diagnose_datum'})
        row['zgt_id'] = DIM_ZORGTYPE.ensure(row, name_mapping)
        row['dia_id_primair'] = DIM_DIAGNOSE.ensure(row, name_mapping)
        row['prg_id'] = DIM_PRODUCTGROEP.ensure(row, name_mapping)
        row['psc_id'] = DIM_PRESTATIECODE.ensure(row, name_mapping)
        row['cct_id'] = DIM_CIRCUIT.ensure(row, name_mapping)
        row['afs_id'] = DIM_AFSLUITREDEN.ensure(row, name_mapping)
        row['lnd_id'] = DIM_LAND.ensure(row, name_mapping)

        # insert fact table
        FCT_SUBTRAJECT.insert(row, name_mapping)

    connection.commit()

    end_s = time.time()
    endtime = time.localtime()
    print('{} - Finished processing {}'.
          format(time.strftime('%H:%M:%S', endtime), file))
    print('           Processing time: %0.2f seconds ' % (end_s - start_s))


def main():
    """ Main routine for loading WOB GGZ subtrajecten."""
    global cnx, cur

    # trunctate FCT.SUBTRAJECT
    cur.execute("truncate table FCT.SUBTRAJECT")
    cnx.commit()

    # loop to load per file: DOT
    data_path = config.get('wob_ggz', 'data_path')
    files = [fn for fn in os.listdir(data_path)
                 if any([fn.endswith(ext) for ext in ['bz2']] and
                        [fn.startswith(name) for name in
                         ['DIS_RAP_GGZ_WOB_DBC_500_20140429_']])]

    for file in files:
            load_subtraject(file, config)

    cnx.close()


if __name__ == "__main__":
    main()
