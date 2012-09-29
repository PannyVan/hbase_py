#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-07

import sys
sys.path.append("../jk_hbase_api")
import time
import datetime
import random

from JKHBase import JKHBase

if __name__ == "__main__":
    testNum = 100
    writeType = 'one' #batch for batch
    if len(sys.argv) > 1:
        testNum = int(sys.argv[1])
    if len(sys.argv) > 2:
        writeType = sys.argv[2] 

    random.seed()
    lines = []
    for i in xrange(testNum):
        jkid = random.randrange(1,10000+1)
        line = "jkid=%d`time=20200701000000|key1=value1`key2=value2" % jkid
        lines.append(line)
    table=JKHBase.getTable("t_m1")
    t0 = time.time()
    total = 0
    if writeType == "one":
        for line in lines:
            table.write(line)
            total += 1
    else:
        table.writeAll(lines)
        total += len(lines)
    t1 = time.time()
    tt = t1 - t0
    print "write %d, use %f second, %d/s" % (total, tt, (total/tt))
