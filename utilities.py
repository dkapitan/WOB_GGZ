""" Utilities for WOB GGZ ETL based on pygrametl framework.

Various utilities for:
- data-munging of WOB ZZ dataset into the right format
- MS SQL Server specific methods on top of pygrametl
"""

from decimal import *
import pandas as pd

__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'


def parse_boolean(value, default=None):
    """Method for parsing boolean 'J'/'N'/leeg into 1,0 or null."""
    if value == None:
        return None
    elif value.upper() == 'J':
        return 1
    elif value.upper() == 'N':
        return 0
    else:
        return default


def parse_codes(value, zfill_length, default='?'):
    """Method for parsing varchar codes.

    Arguments:
    - zfill_length: length to which value should be left-padded with zeros
    - default: value for nulls/blanks
    """
    if value in ['','0']:
        return default
    else:
        try:
            value = value.upper()
            return str(value).zfill(zfill_length)
        except Exception:
            return default


def parse_dates(value, default='10000101'):
    """ Method for parsing dates to ISO format.

    Assumes input data is of format 'YYYYMMDD' with output 'YYYY-MM-DD'
    Blank dates are set to 1000-01-01.
    """
    if value == None:
        value = default
    try:
        value = str(value)
        return '-'.join([value[0:4], value[4:6], value[6:8]])
    except Exception:
        return '-'.join([default[0:4], default[4:6], default[6:8]])


def parse_nulls(string, default='0000'):
    """ Check for nulls or blanks, return default if found."""
    if pd.isnull(string):
        return default
    else:
        return string.zfill(4)


def parse_money(value, default=Decimal(0)):
    """Method for parsing money values from cents to decimals."""
    try:
        return Decimal(value)/Decimal(100)
    except Exception:
        return default


def datetime_to_mssql_string(datetime, default='1000-04-04 00:00:00'):
    """ parse dates in mssql datetime string format with 1000-04-04 for blanks"""
    try:
        return datetime.strftime('%Y-%m-%d') + (' 00:00:00')
    except Exception:
        return default


def get_columns(db, schema, table, cursor):
    """ Get columns in right order from a table."""
    stmt = ('''select column_name from information_schema.columns
               where table_catalog = '{}'
               and   table_schema = '{}'
               and   table_name = '{}'
            ''')
    cursor.execute(stmt.format(db, schema, table))
    result = pd.DataFrame(cursor.fetchall())
    return result[0].tolist()


def get_column_types(db, schema, table, cursor):
    """ Get columns in dataframe in sorted order from a table."""
    stmt = '''select column_name,
                      data_type,
                      character_maximum_length
               from information_schema.columns
               where table_catalog = '{}'
               and   table_schema = '{}'
               and   table_name = '{}'
            '''
    cursor.execute(stmt.format(db, schema, table))
    result = pd.DataFrame(cursor.fetchall())
    return result


def map_missing_values(x, data_type, length):
    """ Fill missing values with DIKW convention -1, ... -4 """
    na_rep = {-1: '?', -2: '#', -3: '~', -4: '^' }
    int_types = ['smallint', 'int', 'bigint']
    char_types = ['char', 'nchar', 'varchar', 'nvarchar']
    date_types = ['date', 'datetime']
    if data_type in int_types: na = x
    elif data_type in date_types: na = '1000-0{0}-0{0}'.format(str(x)[-1])
    elif data_type in char_types:
        if length == 1:     na = na_rep[x]
        elif length == 2:   na = na_rep[x]*2
        elif length > 2:    na = '_{}_'.format(na_rep[x])
        else: None
    elif data_type == 'bit': na = None
    elif data_type == 'tinyint': na = None
    else: None
    return na


def create_missing_values(db, schema, table, cursor):
    """ Generates dataframe with missing records for db.schema.table
    """
    na_records = get_column_types(db, schema, table, cursor)
    for x in range(-1, -5, -1):
        na_records[x] = na_records.apply(lambda row: map_missing_values(x, row[1], row[2]), axis=1)
    na_records = na_records.drop([1,2], axis=1)
    na_records = na_records.transpose()
    na_records.columns = na_records.ix[0,]
    na_records = na_records.drop([0], axis=0)
    return na_records


def map_missing_values_tiny(x, data_type, length):
    """ Fill missing values with _id = 0 for tiny dimensions """
    na_rep = {0: '?'}
    int_types = ['smallint', 'int', 'bigint']
    char_types = ['char', 'nchar', 'varchar', 'nvarchar']
    date_types = ['date', 'datetime']
    if data_type in int_types: na = x
    elif data_type in date_types: na = '1000-0{0}-0{0}'.format(str(x)[-1])
    elif data_type in char_types:
        if length == 1:     na = na_rep[x]
        elif length == 2:   na = na_rep[x]*2
        elif length > 2:    na = '_{}_'.format(na_rep[x])
        else: None
    elif data_type == 'bit': na = None
    elif data_type == 'tinyint': na = 0
    else: None
    return na


def create_missing_values_tiny(db, schema, table, cursor):
    """ Generates dataframe with missing records for db.schema.table

    For tiny dimension with tinyint as primary key. Unknown value _id=0
    """
    na_records = get_column_types(db, schema, table, cursor)

    # dirty hacks: assumes less than 255 records for db.schema.table)
    na_records[256] = na_records.apply(
        lambda row: map_missing_values_tiny(0, row[1], row[2]), axis=1)
    na_records = na_records.drop([1,2], axis=1)
    na_records = na_records.transpose()
    na_records.columns = na_records.ix[0,]
    na_records = na_records.drop([0], axis=0)
    return na_records

