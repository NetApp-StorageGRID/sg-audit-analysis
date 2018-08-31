#!/usr/bin/env python3

import pandas as pd
import numpy  as np
import re as re
import csv
import argparse
from pathlib import Path





def process_one_audit_log_line(line):

    myLine = dict()

    # remove all types. Replace "(some text)" with ""
    new_line = re.sub(r'\([a-zA-Z0-9_]*\)', '', line)

    #pull out the time
    s = re.split(" ", new_line, 1)
    time = s[0]
    remaining = s[1]

    myLine['Timestamp'] = time

    #print ("Time: " + time)
    #print ("Remaining After Time: " + remaining)


    #pull out the audit message [AUDT: <pulling out audit>]
    m = re.match(r'\[AUDT\:(.*)\]', remaining)
    audit_msg = m.group(1)
    #print ("Audit Msg: " + audit_msg)

    # pull out each element
    n = re.findall(r'(\[(.*?)\])', audit_msg)

    for i in n:
        # split into key and value
        keyval = i[1]

        #print("Keyval = ", keyval)

        o = re.match(r'(.{4})\:(.*)', keyval)
        key = o.group(1)
        val = o.group(2)

        myLine[key] = val

        #print ("Key = " + key + ", Val = " + val)
        #print (myLine[key])

    return myLine





#### Main Starts Here

parser = argparse.ArgumentParser(description='Convert StorageGRID Audit to CSV')
parser.add_argument("source_file", help="Audit log file to convert to csv", type = str)
parser.add_argument("destination_file", help="The CSV file to generate", type = str)

args = parser.parse_args()




fieldnames = ['Timestamp','HSID','CNID','OBCL','RSLT',\
              'ANID', 'AVER', 'ATYP', 'ASQN', 'ATID', \
              'ATIM', 'ASES', 'AMID','SVIP', 'SEID', \
              'SAIP', 'CNDR', 'DAIP','INIE',\
             'TIME', 'SBAC', 'S3AI', 'SBAI','SACC', \
              'S3BK', 'S3AK','SUSR', 'CBID', 'UUID', \
              'S3KY', 'CSIZ', 'PATH', 'RULE', 'LOCS', \
              'STAT', 'SPAR', 'MPAT', 'MRMD', 'MDNA', \
              'MUUN', 'MSIP', 'MPQP', 'MRSP', 'MRBD', \
              'MRSC', 'MDIP', 'ULID', 'SEGC', 'SGCB', \
              'HTRH','CTSS', 'CTDS', 'CTSR', 'CTDR', 'CTAS', 'CTES']

# check if the output file already exists. We don't want to overwrite
if Path(args.destination_file).is_file():
    print("Error: destination file exists. We will not overwrite");
    exit(1)


with open(args.destination_file, 'w') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames = fieldnames)
    writer.writeheader()

    with open(args.source_file, "r") as f:
        for l in f:
            myRow = process_one_audit_log_line(l)
            writer.writerow(myRow)

