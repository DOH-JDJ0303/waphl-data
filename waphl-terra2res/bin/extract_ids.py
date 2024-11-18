#!/usr/bin/env python

import argparse
import re
#----- ARGUMENTS -----#
parser = argparse.ArgumentParser(
                    prog='get_timestamp.py',
                    description='Get Google object timestamp.')
parser.add_argument('-i',
                    '--input',
                    help = 'Google file')
parser.add_argument('--patterns',
                    nargs = "*",
                    help = 'String patterns that should be extracted from the sample name. Only samples that match one of these pattern will be transferred. Multiple patterns separated by spaces can be supplied (Default: "*").')
args = parser.parse_args()

#------ GET SAMPLEID ------#
id = None
for pattern in args.patterns:
    match = re.search(pattern, args.input)
    if match:
        id = match.group()

print(id)

# write file
f = open("ids.csv", "w")
f.write(str(id))
f.close()