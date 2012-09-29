#!/usr/bin/env python
#coding:utf8
#author:abloz.com
#date:2012.6.1

import sys
sys.path.append('./gen-py')

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import *

transport = TSocket.TSocket('localhost', 9090)
transport = TTransport.TBufferedTransport(transport)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hbase.Client(protocol)
transport.open()

print client.getTableNames()

tableName="test"

try:
    client.createTable(tableName,[ColumnDescriptor(name="info:", maxVersions=1)])
except AlreadyExists:
    client.disableTable(tableName)
    client.deleteTable(tableName)
    client.createTable(tableName,[ColumnDescriptor(name="info:", maxVersions=1)])

print client.getTableNames()

mutations = [Mutation(column="info:", value="content")]
client.mutateRow(tableName,"1", mutations)
print client.getRow(tableName,"1")

