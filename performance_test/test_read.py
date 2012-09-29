#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-07
"""
测试读性能，单进程，模拟常用读方法测试 jk hbase api 接口的读性能
"""
import sys
sys.path.append("../jk_hbase_api")
import time
import datetime
import random

from JKHBase import JKHBase

if __name__ == "__main__":
    testNum = 10
    random.seed()
    table = JKHBase.getTable('t_m1')
    days = 10
    if len(sys.argv)>1:
        testNum = int(sys.argv[1])
    if len(sys.argv)>2:
        days = int(sys.argv[2])
    time1ds = datetime.datetime(2012,7,13,0,0,0)
    time1de = time1ds + datetime.timedelta(days=days) + datetime.timedelta(seconds=-1)
    print "%s - %s" % (time1ds.strftime("%Y-%m-%d %H:%M:%S"), time1de.strftime("%Y-%m-%d %H:%M:%S"))
    ts1ds = time.mktime(time1ds.timetuple())
    ts1de = time.mktime(time1de.timetuple())
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.time()
    total = 0
    jkids = []
    for i in xrange(testNum):
        jkid = random.randrange(1,10000+1,1)
        jkids.append(jkid)

    for i in xrange(testNum):
        print "read %d/%d" % (i+1, testNum)
        #print "jkid = %d" % jkid
        r = table.read(jkids[i], None, ts1ds, ts1de)
        l = len(r)
        #print "len=%d" % l
        total += l

    print datetime.datetime.now()
    t1 = time.time()
    tt = t1 - t0
    print "write %d, use %d second, %d/s" % (total, tt, (total/tt))
