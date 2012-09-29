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
import MySQLdb

data_dir = "./data"

file = "%s/1.log" % data_dir

f = open(file, "w+")
f.write("jkid=1`time=20120614113001`check_http监控异常")
f.write("\n")
f.write("jkid=2`time=20120614113001`check_http监控异常")
f.write("\n")
f.write("jkid=1`time=20120614113102`check_http监控异常")
f.write("\n")
f.close()

subprocess.call(["mysql_store.py -flagId 1 -runTime 0"],shell=True)

conn = MySQLdb.Connection(host='platform32', port=23306, user='root', passwd='root', db='jkerror', charset='utf8')
#conn.select_db('database name')
cur = conn.cursor()
cur.execute('select * from t_error')
#row=cur.fetchone()
#print row
#cur.scroll(0,'absolute')
row=cur.fetchall()
print row
#cur.execute("insert  into table (row1, row2) values ('111', '222')")
#cur.execute("update  table set   row1 = 'test'  where  row2 = 'row2' ")
#cur.execute("delete from  table  where row1 = 'row1' ")
#cur.execute("update  table set   row1 = '%s'  where  row2 = '%s' " %('value1','value2'))
#cur.execute("update FTPUSERS set passwd=%s where userid='%s' " %("md5('123')",'user2'))
#cur.execute("update FTPUSERS set passwd=%s where userid='%s' " %("md5('123')",'user2'))
#conn.commit()
cur.close()
conn.close()

