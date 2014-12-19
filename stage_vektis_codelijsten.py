""" Prepares vektis COD016 reference for ETL in WOB_ZZ DWH.

Reference table of all specialisme-soorten and instelling soorten.
Manually enriched with abreviations and short descriptions
"""

import configparser
import pymssql as sql
import pandas as pd

__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'


def main():

    # setup database connection
    config = configparser.ConfigParser()
    config.read('/opt/projects/wob_ggz/config.ini')
    login = {
        'user': config.get('local_mssql', 'user'),
        'password': config.get('local_mssql', 'password'),
        'server': config.get('local_mssql', 'server'),
        'port': config.get('local_mssql', 'port'),
        'database': config.get('wob_ggz', 'database')}
    cnx = sql.connect(**login)
    cursor = cnx.cursor()

    # configure files
    paths = {'vektis_path': config.get('wob_ggz', 'vektis_path'),
             'staging_path': config.get('wob_ggz', 'staging_path')}
    data_file = paths['vektis_path'] + '/COD032_-_NEN.csv'
    data = pd.read_csv(data_file, sep=';',dtype=str, encoding='cp1252')

     # write output to .csv
    data.to_csv(paths['staging_path'] + '/DIM.LAND.csv', sep=';',
              header=True, index=False, encoding='cp1252',
              quoting=None, na_rep='')

if __name__ == '__main__':
    main()