#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-07

import sys
sys.path.append("../jk_hbase_api")

import time
import os
import subprocess

data_dir = "./data"

file = "%s/1.log" % data_dir

f = open(file, "w+")
f.write("jkid=merge1`time=20120614113001|key1=1`key2=2")
f.write("\n")
f.write("jkid=merge1`time=20120614113101|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614113201|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614113301|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614113401|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614113501|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614113601|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614113701|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614113801|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614113901|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614114001|key1=1`key2=2")
f.write("\n")
f.write("jkid=merge1`time=20120614114101|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614114201|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614114301|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614114401|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614114501|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614114601|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614114701|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614114801|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614114901|key1=2`key2=3")
f.write("\n")
f.write("jkid=merge1`time=20120614115001|key1=2")
f.write("\n")
f.close()

# 21行，key1=(1+9*2)*2+2=40，40/21=1.9047  1.9<x<2.0
# 20行，key2=(2+9*3)*2=58, 58/20=2.9

subprocess.call(["../jk_hbase_api/create_table.py"],shell=True)
subprocess.call(["../hbase_store/hbase_store.py -flagId 1 -runTime 0"],shell=True)
print "merge m2h"
subprocess.call(["hbase_merge.py -type m2h -day 20120614 -time 11"],shell=True)
print "merge h2d"
subprocess.call(["hbase_merge.py -type h2d -day 20120614"],shell=True)

from JKHBase import JKHBase

table = JKHBase.getTable("t_h1")
print "read merge1:"
print table.read("merge1",None,0,time.time())
table = JKHBase.getTable("t_d1")
print "read merge1:"
print table.read("merge1",None,0,time.time())
