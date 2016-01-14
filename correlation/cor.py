import os
import csv
import sys
import math
import time
import numpy as np
import array
from scipy.stats.stats import pearsonr
from scipy.interpolate import interp1d
import base64
import random
import logging
import argparse
import logging.handlers

import datetime

from fractions import gcd
from tabulate import tabulate

log = logging.getLogger("Crypto")

def setup_logging(debug):
    formatter = logging.Formatter("%(message)s")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    if debug:
        log.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)
        ch.setLevel(logging.INFO)

    syslog = logging.handlers.SysLogHandler(address='/dev/log')
    syslog.setLevel(logging.WARN)
    log.addHandler(syslog)
    log.addHandler(ch)

def setup_arguments():
    parser = argparse.ArgumentParser(description='Crypto Assignment 3')
    parser.add_argument('-d', action='store_true', dest='debug',default=False, help='Enable debug logging')
    return parser.parse_args()


def main():

    args = setup_arguments()
    setup_logging(args.debug)

    count_rows = 0
    records = list()

    cur_cbs = 0
    time_lst = list()
    count_lst = list()

    with open('results2.csv', 'r') as csvfile:
        results = csv.reader(csvfile, delimiter=',')
        for row in results:
            count_rows += 1
            if count_rows == 0:
                continue
            try:
                date = datetime.datetime.strptime(row[8],"%d/%m/%Y")
            except Exception as error:
                print("Error",error)
                continue

            cbs = row[9]
            zzp = row[10]

            if not cbs == "#N/A":
                print(date.timestamp())
                timestamp = date.timestamp()
                time_lst.append(timestamp)
                count_lst.append(cbs)

            if zzp == "#N/A":
                continue
            record = [date,cbs,zzp]
            records.append(record)

    records.sort(key=lambda r: r[0])
    x = array.array('i',(int(s) for s in time_lst))
    y = array.array('i',(int(s) for s in count_lst))
    f = interp1d(x,y)

    for record in records:
        if record[1] == "#N/A":
            cbs = f(record[0].timestamp())
            record[1] = cbs
        print("{0}\t{1}\t{2}".format(record[0],int(record[1]),int(float(record[2]))))

    cbs_array = array.array('i',(int(s[1]) for s in records))
    zzp_array = array.array('i',(int(float(s[2])) for s in records))
    result = pearsonr(cbs_array,zzp_array)
    print(result)
    log.info("\nCrypto Ass3 stopped")

if __name__ == "__main__":
    main()

