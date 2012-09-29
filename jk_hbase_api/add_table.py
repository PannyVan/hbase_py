#!/usr/bin/env python
# coding=utf8
# author:abloz.com
# date:2012.6.1

import sys
import os
__DIR__ = os.path.dirname(os.path.realpath(__file__))
sys.path.append("%s/gen-py" % __DIR__)
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *

def create_all_tables(client):
    '''
    t_status_h1 
    t_status_d1
    t_status_h1_group
    t_status_d1_group
    '''
    col_name = ['info:']
    table_name = 't_status_h1'
    create_table(client, table_name, col_name)
    table_name2 = 't_status_d1'
    create_table(client, table_name2, col_name)
    table_name3 = 't_status_h1_group'
    create_table(client, table_name3, col_name)
    table_name4 = 't_status_d1_group'
    create_table(client, table_name4, col_name)

def create_table(client, table_name, table_desc):
    columns = []
    for name in table_desc:
        col = ColumnDescriptor(name, maxVersions=1, compression="NONE")
        columns.append(col)
 
    try:
        print "creating table: %s" %(table_name)
        client.createTable(table_name, columns)
    except AlreadyExists, ae:
        print "WARN: " + ae.message
    except Exception, e:
        print "error happend: " + str(e)
            
def list_all_tables(client):
    # Scan all tables
    print "scanning tables..."
    for t in client.getTableNames():
        print "  -- %s" %(t)
        cols = client.getColumnDescriptors(t)
        print "  -- -- column families in %s" %(t)
        for col_name in cols.keys():
            col = cols[col_name]
            print "  -- -- -- column: %s, maxVer: %d" % (col.name, col.maxVersions)

def connect_hbase():
    transport = TSocket.TSocket('localhost', 9090)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Hbase.Client(protocol)
    transport.open()
    return client

def printRow(entry):
    print "row: " + entry.row + ", cols:",
    for k in sorted(entry.columns):
        print k + " => " + entry.columns[k].value,
        print "/n"

def scann_table(client):
    t = 'store_table'
    columnNames = []
    for (col, desc) in client.getColumnDescriptors(t).items():
        print "column with name: "+desc.name
        print desc
        columnNames.append(desc.name+":")
        print columnNames

        print "Starting scanner..."
        scanner = client.scannerOpenWithStop(t, "", "", columnNames)

        r = client.scannerGet(scanner)
        i = 0
        while r:
            if i % 100 == 0:
                printRow(r[0])
                r = client.scannerGet(scanner)
                i += 1

        client.scannerClose(scanner)
        print "Scanner finished, total %d row" % i

if __name__ == '__main__':
    client = connect_hbase()
    create_all_tables(client)
    list_all_tables(client)
    #scann_table(client)
