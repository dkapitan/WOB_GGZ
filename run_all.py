#!/usr/bin/env python
""" Script to run whole ETL for WOB_GGZ.

All steps are run sequentially, parallel loading may be developed
in the future.
"""

import wob_ggz

__author__ = 'Daniel Kapitan'
__maintainer__ = 'Daniel Kapitan'
__version__ = '0.1'

if __name__  == '__main__':
    wob_ggz.create_tables.main()
    wob_ggz.stage_date_dimensions.main()
    wob_ggz.stage_dbc_codelijsten.main()
    wob_ggz.stage_vektis_codelijsten.main()
    wob_ggz.load_staged_dimensions.main()
    wob_ggz.load_fct_subtraject.main()
    wob_ggz.create_indexes.main()


