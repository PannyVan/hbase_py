#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-07
"""
测试读性能，多线程
"""
import sys
sys.path.append("../jk_hbase_api")
import time
import datetime
import random
import threading

from JKHBase import JKHBase

_testNum = 0
_testTotal = 0
_ts1ds = None
_ts1de = None
_readTotalRow = 0

class Worker(threading.Thread):
    def __init__(self, threadname, jkids):
        threading.Thread.__init__(self, name = threadname)
        self.jkids = jkids
        self.table = JKHBase.getTable('t_m1')

    def run(self):
        global _ts1ds
        global _ts1de
        print "%s start" % self.getName()
        global _testNum
        global _testTotal
        global _readTotalRow

        print self.jkids
        for jkid in self.jkids:
            r = self.table.read(jkid, None, _ts1ds, _ts1de)
            le = len(r)
            _readTotalRow += le
            _testNum += 1
            print "read %d/%d" % (_testNum, _testTotal)
        print "%s end" % self.getName()

if __name__ == "__main__":
    threadNum = 10
    threadTestNum = 30
    days = 1
   
    global _ts1ds
    global _ts1de
    global _testTotal
    global _readTotalRow

    _testTotal = threadNum * threadTestNum
    random.seed()
    if len(sys.argv)>1:
        testNum = int(sys.argv[1])
    if len(sys.argv)>2:
        days = int(sys.argv[2])
    
    time1ds = datetime.datetime(2012,7,13,0,0,0)
    time1de = time1ds + datetime.timedelta(days=days) + datetime.timedelta(seconds=-1)
    print "%s - %s" % (time1ds.strftime("%Y-%m-%d %H:%M:%S"), time1de.strftime("%Y-%m-%d %H:%M:%S"))
    _ts1ds = time.mktime(time1ds.timetuple())
    _ts1de = time.mktime(time1de.timetuple())
    print datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.time()
    
    workers = []
    for i in xrange(threadNum):
        jkids = []
        for j in xrange(threadTestNum):
            jkid = random.randrange(1,10000+1,1)
            jkids.append(jkid)
        worker = Worker(("work-%d" % i), jkids)
        workers.append(worker)

    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join()

    print datetime.datetime.now()
    t1 = time.time()
    tt = t1 - t0
    print "read %d, use %d second, %d/s" % (_testTotal, tt, (_testTotal/tt))
    print "read %d, use %d second, %d/s" % (_readTotalRow, tt, (_readTotalRow/tt))
