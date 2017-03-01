#!/usr/local/bin/python3

import sys
import cmd
import csv

import pdb

import config
from zuora import Zuora

zuora = Zuora(config.zuoraConfig)


def zuoraObjectKeys(zouraObject):
    if zouraObject:
        return zouraObject.keys()
    
def dumpRecords(records):
    if records:
        firstRecord = records[0]
        keys = [key for key in zuoraObjectKeys(firstRecord) if firstRecord[key]]
        
        print(','.join(keys))
        
        for record in records:
            print(','.join(str(record[key]) for key in keys))
            
        print(len(records), 'records')

class Interpeter(cmd.Cmd):
    def do_select(self, line):
        try:
            if '.' in line:
                csvData = zuora.queryExport('select ' + line).split('\n')
                records = [record for record in csv.DictReader(csvData)]
            else:
                records = zuora.queryAll('select ' + line)
            dumpRecords(records)
        except Exception as e:
            print('Error: q', repr(e))

    def do_q(self, line):
        return self.do_EOF(line)

    def do_EOF(self, line):
        return True

if __name__ == '__main__':
    Interpeter().cmdloop()
