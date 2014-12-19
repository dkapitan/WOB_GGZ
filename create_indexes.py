""" Create indexes on WOB GGZ star schema.

Basic indexing strategy:
    - Create your fact table clustered index on the date key.
      The majority of (typical) fact table queries include a time element
      and clustering on the date key enables range scanning of fact table rows.
    - Add non-clustered indexes on the foreign keys of your fact tables
      to assist with highly selective queries. Foreign keys to dimension tables
      can be created with NOCHECK to prevent any impact on ETL.
    - Cluster your dimension tables on their surrogate keys. This is done
      automatically by defining primary key in DDL
    - Create a non-clustered index on the natural key of each dimension table.
      This is done automatically by defining UNIQUE in DDL
"""


import pymssql as sql
import configparser
import os

__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'

def create_index(table, column, cursor, clustered=False):
    """Create index on single column on table

    - full table name should read DB.SCHEMA.TABLENAME
    - Indexname same as column name
    - By default non-clustered
    - Existing index on table with same column-name is dropped
    - assumes cursor has autocommit
    """

    if clustered == True: clusteroption = 'clustered'
    else: clusteroption = 'nonclustered'

    # drop index if exists
    stmt = '''
    if exists
        (select 'True' from sysindexes
         where id = (select object_id('{}'))
         and name = '{}')
    drop index {} on {}
    '''.format(table, column, column, table)
    cursor.execute(stmt)

    stmt = '''
    create {} index {}
    on {} ({})
    '''.format(clusteroption, column, table, column,)
    cursor.execute(stmt)

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

    # turn autocommit on
    cnx.autocommit(True)
    cursor = cnx.cursor()

    # create clustered index on FCT_SUBTRAJECT on begindatum_subtraject
    create_index('WOB_GGZ.FCT.SUBTRAJECT',
                 'dag_id_begindatum_subtraject',
                 cursor,
                 clustered=True)

    # create nonclustered indexes on FCT_SUBTRAJECT dimension_ids
    for dim in ['dag_id_einddatum_subtraject', 'zgt_id', 'dia_id_primair',
                'prg_id', 'psc_id', 'cct_id', 'afs_id']:
        create_index('WOB_GGZ.FCT.SUBTRAJECT', dim, cursor)

    # all dimensions have clustered index on PK,
    # and nonclustered index on unique natural keys (can also be combined columns)


if __name__ == "__main__":
    main()