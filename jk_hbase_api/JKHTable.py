#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-07

import sys
import os
__DIR__ = os.path.dirname(os.path.realpath(__file__))
sys.path.append("%s/gen-py" % __DIR__)
import time

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation

os.environ["TZ"]="Asia/Shanghai"
try:
    time.tzset()
except AttributeError:
    pass

class JKHTable:
    """
    为了简单易用，屏蔽中间信息。
    """
    def __init__(self, netloc="localhost", port=9090, table="t_m1"):
        self.tableName = table
        
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(netloc, port))
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)
        self.transport.open()
        
        #print self.client.getTableNames()
    
    def __del__(self):
        self.transport.close()

    #重要：注意时间的精确度 YYYYmmddHHMMSS 14位
    def write(self, info):
        """
        info:jkid=12345`time=20120614113001|key1=value1`key2=value2
        return:jkid
        """
        if info is None:
            raise ValueError, "info is None"
        (key, value) = info.split('|', 2)
        kvMap = {}
        for kv in key.split('`'):
            (k, v) = kv.split('=')
            kvMap[k] = v

        jkid = kvMap['jkid']
        jktime = kvMap['time']
        keyTime = jktime[0:12]
        rowKey = "%s_%s" % (jkid, keyTime)
        #print rowKey
        cols=[]
        
        for kv in value.split('`'):
            k, v = kv.split('=')
            colName = "info:%s" % k
            col = Mutation(column=colName, value=v)
            cols.append(col)
        
        timestamp = time.mktime(time.strptime(jktime, "%Y%m%d%H%M%S"))
        self.client.mutateRowTs(self.tableName, rowKey, cols, timestamp)
        return jkid
    
    def writeAll(self, infos):
        """
        same as write, but write multi lines
        return:None
        NOTE:all Mutation using one timestamp, use the bigest line's timestamp
        """
        if infos is None:
            raise ValueError, "info is None"
        rowBatches = []
        theBigestTime = None
        for info in infos:
            batchMutation = BatchMutation()
            (key, value) = info.split('|', 2)
            kvMap = {}
            for kv in key.split('`'):
                (k, v) = kv.split('=')
                kvMap[k] = v

            jkid = kvMap['jkid']
            jktime = kvMap['time']
            keyTime = jktime[0:12]
            rowKey = "%s_%s" % (jkid, keyTime)
            batchMutation.row = rowKey
            #print rowKey
            cols=[]
            
            for kv in value.split('`'):
                k, v = kv.split('=')
                colName = "info:%s" % k
                col = Mutation(column=colName, value=v)
                cols.append(col)
            batchMutation.mutations = cols
            if cmp(theBigestTime, jktime) < 0:
                theBigestTime = jktime
            rowBatches.append(batchMutation)
        
        if theBigestTime is not None: 
            timestamp = time.mktime(time.strptime(theBigestTime, "%Y%m%d%H%M%S"))
            self.client.mutateRowsTs(self.tableName, rowBatches, timestamp)
            #print "writeAll %s" % theBigestTime
    
    #非常重要，endTime是timestamp，并且是半闭半开区间 
    #rowkey是 id_YYYYmmddHHMM组成的，注意时间的精确度
    def read(self,jkid,columns,startTime,endTime):
        """
        jkid:jkid or None, None means all rows
        columns:[string,string...] or None, None means all col
        startTime:unix timestamp, [startTime, endTime)
        return: [{jkid:12345, keyTime:201206141130, time:20120614113001, columns:{key1:value1, key2:value2}},...] 
        return: if specify columns is not exists, return []
        """
        if columns is not None:
            i = 0
            colNum = len(columns)
            while i < colNum:
                columns[i] = "info:%s" % columns[i]
                i += 1
        
        scanner = None
        if jkid is None:
            scanner = self.client.scannerOpen(self.tableName, "", columns)
        else:
            #start = time.strftime("%Y%m%d%H%M",time.gmtime(startTime))
            start = time.strftime("%Y%m%d%H%M",time.localtime(startTime))
            #end = time.strftime("%Y%m%d%H%M",time.gmtime(endTime))
            end = time.strftime("%Y%m%d%H%M",time.localtime(endTime))
            startRowKey = "%s_%s" % (jkid, start)
            endRowKey = "%s_%s" % (jkid, end)
            scanner = self.client.scannerOpenWithStop(self.tableName, startRowKey, endRowKey, columns)

        r = self.client.scannerGet(scanner)
        result= []

        while r:
            #print r
            """
            [TRowResult(columns={'info:key1': TCell(timestamp=1341316673368L, value='value1'), \
            'info:key2': TCell(timestamp=1341316673368L, value='value2')}, row='12345_201206141130')]
            """
            trr = r[0]
            jkid, keyTime = trr.row.rsplit('_',1)
            kvMap = {'jkid':jkid, 'keyTime':keyTime}
            timestamp=None
            colMap = {}
            for column in trr.columns:
                k = column.split(':', 2)[1]
                tcell = trr.columns[column]
                v = tcell.value
                colMap[k] = v
                if timestamp is None:
                    timestamp = tcell.timestamp
                    kvMap['timestamp'] = timestamp
            
            kvMap['columns'] = colMap
            result.append(kvMap)
            r = self.client.scannerGet(scanner)

        self.client.scannerClose(scanner)
        return result

    def delete(self, jkid, startTime, endTime):
        pass

if __name__ == "__main__":
    pass
