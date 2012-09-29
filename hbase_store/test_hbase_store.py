#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-07

import sys
sys.path.append("../jk_hbase_api")

import time
import os
import os.path
import subprocess

data_dir = "./data"

file = "%s/1.log" % data_dir

f = open(file, "w+")
f.write("jkid=1`time=20120614113001|key1=value1`key2=value2")
f.write("\n")
f.write("jkid=2`time=20120614113001|key1=value1`key2=value2`key3=value3")
f.write("\n")
f.write("jkid=3`time=20120614113001|key1=value1`key2=value2`key4=value4`key5=value5")
f.write("\n")
f.close()

#subprocess.call(["hbase_store.py -flagId 1 -runTime 60"],shell=True)
subprocess.call(["hbase_store.py -flagId 1 -runTime 0"],shell=True)

from JKHBase import JKHBase

table = JKHBase.getTable("t_m1")
print "read 1:"
print table.read("1",None,0,time.time())
print "read 1 key1:"
print table.read("1",["key1"],0,time.time())
print "read 3 key3:"
print table.read("3",["key3"],0,time.time())
