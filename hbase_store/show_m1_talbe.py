#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-07

import time
import os 
import sys 
sys.path.append("../jk_hbase_api")

from JKHBase import JKHBase

if __name__ == "__main__":
    table=JKHBase.getTable("t_m1")
    jkidFile = "./logs/1.jkid.list.txt"
    if os.path.exists(jkidFile):
        f = open(jkidFile, "r")
        for line in f:
            line=line.strip('\n')
            if len(line) <=0: continue
            print line
            t2 = time.time()
            t1 = t2 - (1000000 * 60 )
            print t1, t2
            rs=table.read(line,None,t1,t2)
            for r in rs:
                print r
        f.close()
