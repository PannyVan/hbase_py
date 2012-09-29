#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-07

import time
import pdb

from JKHBase import JKHBase
#pdb.set_trace()
table=JKHBase.getTable("t_m1")
#   table.write("jkid=12345`time=20120614113001|key1=value1`key2=value2")
rs=table.read("1_BJAASR01147",None,0,time.time())
for r in rs:
    print r
