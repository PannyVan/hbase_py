#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-07
import os
from JKHTable import JKHTable
__DIR__ = os.path.dirname(os.path.realpath(__file__))

class JKHBase:
    netloc = None
    port = None
    
    @classmethod 
    def getTable(cls, tableName):
        global __DIR__
        if cls.netloc is None:
            f = None
            try:
                f = open(os.path.join(__DIR__, 'hbase.host.txt'))
                for line in f:
                    line = line.strip()
                    if line.startswith("#"):
                        continue
                    cls.netloc,cls.port = line.split(':')
                    break
                f.close
            finally:
                if f is not None:
                    f.close()
        table = JKHTable(cls.netloc,cls.port,tableName);
        return table;

if __name__ == "__main__":
    pass
