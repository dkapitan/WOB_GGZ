""" DDL to prepare tables on MSSQL server for ETL of WOB_GGZ DWH.

MSSQL server structure DB.SCHEMA.TABLE is used to separate
fact- and dimension tables.

Because partitioning is not available in
MSSQL server BI edition 2014 (production version), data is partioned
manually per year.

Order of columns is 'logical', i.e.
    - ID columns first
    - most important columns first

NB: no identity declaration for dimension IDs,
since this is managed by pygrametl during load.
"""

import _mssql as sql
import configparser

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
        'database': config.get('wob_ggz', 'database')
    }

    # All columns that are part of unique key are non-nullable;
    # All other columns DEFAULT NULL
    tables = {}
    
    tables['DIM.AFSLUITREDEN'] = ('''
        create table DIM.AFSLUITREDEN (
        afs_id smallint not null primary key,
        afs_afsluitreden_code tinyint not null unique,
        afs_afsluitreden_beschrijving varchar(255) default null,
        afs_afsluitreden_sorteervolgorde varchar(4) default null,
        afs_afsluitreden_mutatie tinyint default null,
        afs_afsluitreden_branche_indicatie tinyint default null
        )
    ''')
    tables['DIM.CIRCUIT'] = ('''
        create table DIM.CIRCUIT (
        cct_id smallint not null primary key,
        cct_circuit_code tinyint not null unique,
        cct_circuit_beschrijving varchar(60) default null,
        cct_circuit_sorteervolgorde tinyint default null,
        cct_circuit_mutatie tinyint default null,
        cct_circuit_branche_indicatie tinyint default null
        )
    ''')

    tables['DIM.DAG'] = ('''
        create table DIM.DAG (
        dag_id smallint not null primary key,
        dag_datum date not null unique,
        dag_jaar smallint default null,
        dag_kwartaal tinyint default null,
        dag_maand tinyint default null,
        dag_week tinyint default null,
        dag_jaar_maand nvarchar(8) default null,
        dag_jaar_week nvarchar(8) default null
        )
    ''')

    tables['DIM.DIAGNOSE'] = ('''
        create table DIM.DIAGNOSE (
        dia_id smallint not null primary key,
        dia_diagnose_code nvarchar(20) not null unique,
        dia_diagnose_groepcode nvarchar(20) default null,
        dia_diagnose_element nvarchar(255) default null,
        dia_diagnose_beschrijving nvarchar(255) default null,
        dia_diagnose_zvz_subscore tinyint default null,
        dia_diagnose_aanspraak_type tinyint default null,
        dia_diagnose_hierarchieniveau tinyint default null,
        dia_diagnose_selecteerbaar tinyint default null,
        dia_diagnose_sorteervolgorde nvarchar(8) default null,
        dia_diagnose_as tinyint default null,
        dia_diagnose_refcode_icd9cm nvarchar(8) default null,
        dia_diagnose_refcode_icd10 nvarchar(8) default null,
        dia_diagnose_prestatieniveau nvarchar(10) default null,
        dia_diagnose_prestatiecode_naamgeving_ggz nvarchar(255) default null,
        dia_diagnose_prestatiecode_naamgeving_fz nvarchar(255) default null,
        dia_diagnose_prestatiecodedeel_ggz nvarchar(4) default null,
        dia_diagnose_prestatiecodedeel_fz nvarchar(4) default null,
        dia_diagnose_mutatie tinyint default null,
        dia_diagnose_branche_indicatie bit default 0,
        )
    ''')

    tables['DIM.LAND'] = ('''
        create table DIM.LAND (
        lnd_id smallint not null primary key,
        lnd_land_code varchar(2) not null unique,
        lnd_land varchar(255) default null
        )
    ''')

    tables['DIM.PRESTATIECODE'] = ('''
        create table DIM.PRESTATIECODE (
        psc_id int not null primary key,
        psc_prestatiecode_agb_code nvarchar(4) default null,
        psc_zorgtype_prestatiecodedeel nvarchar(4) default null,
        psc_diagnose_prestatiecodedeel nvarchar(4) default null,
        psc_prestatiecode_zvz nvarchar(4) default null,
        psc_productgroep_code nvarchar(8) default null,
        psc_dbc_prestatiecode nvarchar(12) default null,
        psc_declaratiecode nvarchar(6) default null,
        psc_prestatiecode_mutatie bit default null,
        constraint UQ__PRESTATIEDECLARATIE unique (psc_dbc_prestatiecode, psc_declaratiecode)
      )
    ''')

    tables['DIM.PRODUCTGROEP'] = ('''
        create table DIM.PRODUCTGROEP (
        prg_id smallint not null primary key,
        prg_productgroep_code varchar(8) not null unique,
        prg_productgroep_code_verblijf nvarchar(4) default null,
        prg_productgroep_code_behandeling nvarchar(4) default null,
        prg_productgroep_type nvarchar(20) default null,
        prg_productgroep_omschrijving_verblijf nvarchar(255) default null,
        prg_productgroep_omschrijving_behandeling nvarchar(255) default null,
        prg_productgroep_beschrijving nvarchar(255) default null,
        prg_productgroep_hierarchieniveau bit default null,
        prg_productgroep_selecteerbaar bit default null,
        prg_productgroep_sorteervolgorde nvarchar(8) default null,
        prg_productgroep_setting nvarchar(20) default null,
        prg_productgroep_categorie nvarchar(255) default null,
        prg_productgroep_lekenvertaling nvarchar(255) default null,
        prg_productgroep_diagnose_blinderen bit default null,
        prg_productgroep_mutatie tinyint default null,
        prg_productgroep_branche_indicatie bit default null
        )
    ''')

    tables['DIM.SUBTRAJECTNUMMER'] = ('''
        create table DIM.SUBTRAJECTNUMMER (
        stn_id int not null primary key,
        stn_subtrajectnummer varchar(30) default null,
        stn_zorgtrajectnummer varchar(30) default null,
        constraint UQ__STN unique (stn_zorgtrajectnummer, stn_subtrajectnummer)
        )
    ''')

    tables['DIM.ZORGTYPE'] = ('''
        create table DIM.ZORGTYPE (
        zgt_id smallint not null primary key,
        zgt_zorgtype_code nvarchar(4) default null unique,
        zgt_zorgtype_groepcode nvarchar(4) default null,
        zgt_zorgtype_element nvarchar(255) default null,
        zgt_zorgtype_beschrijving nvarchar(255) default null,
        zgt_zorgtype_hierarchieniveau tinyint default null,
        zgt_zorgtype_selecteerbaar bit default null,
        zgt_zorgtype_sorteervolgorde nvarchar(4) default null,
        zgt_zorgtype_prestatiecodedeel nvarchar(4) default null,
        zgt_zorgtype_mutatie tinyint default null,
        zgt_zorgtype_branche_indicatie tinyint default null
        )
    ''')

    tables['FCT.SUBTRAJECT'] = ('''
        create table FCT.SUBTRAJECT (
        stn_id int not null default -1,
        dag_id_begindatum_zorgtraject smallint default -4,
        dag_id_einddatum_zorgtraject smallint default -4,
        dag_id_begindatum_subtraject smallint default -4,
        dag_id_einddatum_subtraject smallint default -4,
        dag_id_diagnose smallint default -4,
        zgt_id smallint default -4,
        dia_id_primair smallint default -4,
        prg_id smallint default -4,
        psc_id int default -4,
        cct_id tinyint default 0,
        afs_id tinyint default 0,
        lnd_id smallint default 0,
        geslacht tinyint default 0,
        diagnose_trekken_van bit default null,
        fct_verkoopprijs decimal(11,2) default null,
        fct_tarief decimal(11,2) default null,
        fct_verrekenbedrag decimal(11,2) default null
        )
    ''')


    cnx = sql.connect(**login)

    for name, ddl in tables.items():
        try:
            print('Dropping if exists and creating table {}: '.format(name), end='')
            stmt = "if (exists (select * from information_schema.tables " \
                   "where table_schema = '{}' " \
                   "and table_name = '{}')) " \
                   "drop table {}"
            cnx.execute_non_query(stmt.format(name.split('.')[0], name.split('.')[1], name))
            cnx.execute_non_query(ddl)
        except sql.MSSQLDatabaseException as error:
            raise
            print(error.message)
        else:
            print('OK')

    cnx.close()

if __name__ == '__main__':
    main()
