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
    jkid = int(sys.argv[1])
    jkid2 = int(sys.argv[2])
    print "jkid = %d" % jkid
    print "jkid2 = %d" % jkid2
    table=JKHBase.getTable("t_m1")
    t0 = time.time()
    print t0 
    #jkid = random.randint(1, 10000*100)
    #2012-07-01 00:00:00
    startTime = datetime.datetime(2012,7,1,0,0,0)
    alltime=[]
    TIME_RG=10000*10
    for i in xrange(0,TIME_RG):
        tt = startTime + datetime.timedelta(minutes=i)
        alltime.append(tt.strftime("%Y%m%d%H%M%S"))

    total = 0
    for id in xrange(jkid,jkid2+1):
        print "id=%d" % id
        idt0 = time.time()
        for t in alltime:
            #line = "jkid=12345`time=20120614113001|key1=value1`key2=value2"
            line = "jkid=%d`time=%s|key1=value1`key2=value2" % (id, t)
            #print line
            table.write(line)
            total += 1
            if total % 1000 == 0:
                print "already:%d" % total
                idt1 = time.time()
                idtt = idt1 - idt0
                print "%d/s" % (1000/idtt)
                idt0 = time.time()
        print datetime.datetime.now()
        idt1 = time.time()
        idtt = idt1 - idt0
        print "%d/s" % (TIME_RG/idtt)

    print "total:%d" % total
    t1 = time.time()
    print t1 
    tt = t1 - t0
    print "write %d, use %d second" % (total, tt)
    print "%d/s" % (total/tt)
