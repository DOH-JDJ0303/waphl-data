#!/usr/bin/env python

import argparse
import firecloud.api as fapi

#----- ARGUMENTS -----#
parser = argparse.ArgumentParser(
                    prog='pull_tables.py',
                    description='Get list of tables from Terra.')
parser.add_argument('-p',
                    '--project',
                    help = 'Terra billing project name')
parser.add_argument('-w',
                    '--workspace',
                    help = 'Terra workspace name')
args = parser.parse_args()

#------ DOWNLOAD ALL TABLES FOR WORKSPACE ------#
# get list of tables in workspace
all_tables = fapi.list_entity_types(args.project, args.workspace).json()
select_tables = []
# download tables
for table in all_tables:
    if '_set' not in table:
        select_tables.append(table)

# write file
with open("tables.csv", "w") as outfile:
    outfile.write("\n".join(select_tables))