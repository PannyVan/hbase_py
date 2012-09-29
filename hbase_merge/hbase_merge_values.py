#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-09

__doc__ = """
汇总监控值
====================
仅处理值的合并，可用性的合并在另一个程序中

汇总数据，从分钟数据汇总成小时数据，或者从小时数据汇
总成日数据。

汇总操作，目前仅计算平均值

本程序设计为定时启动运行。
本程序可以使用不同参数并发运行，但是即便是相同参数也
不会有不良影响。

输入参数：
-type:m2h/h2d   -- 分钟数据转为小时数据，小时数据转为日数据
-date:20120614  -- 要统计的日期
-hour:11        -- 要统计的小时，仅对m2h有效
"""

import sys
sys.path.append("../jk_hbase_api")

import time
import os

from JKHBase import JKHBase

_logs_dir = "./logs"

_type = None    #m2h h2d
_date = None    #"20120614"
_hour = None    #"11"

def error_log(msg):
    global _logs_dir
    logFile = "%s/error.log" % _logs_dir
    f = open(logFile,"a+")
    timeStr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = "%s: %s" % (timeStr, msg)
    print line
    f.write(line)
    f.write('\n')
    f.close()

def read_jkids():
    jkids = []
    table = JKHBase.getTable("t_jkid")
    rows = table.read(None, None, 0, time.time()+86400)
    for row in rows:
        jkids.append(row['jkid'])
    return jkids

def main():
    global _type
    global _date
    global _hour

    usage = "usage: %s -type m2h/h2d -date 20120614 [-hour 11]"% sys.argv[0]
    argvLen = len(sys.argv)
    i = 1
    while i < argvLen:
        param = sys.argv[i]
        if param == '-type':
            _type = sys.argv[i+1]
        elif param == '-date':
            _date = sys.argv[i+1]
        elif param == '-hour':
            _hour = sys.argv[i+1]
            if len(_hour) == 1:
                _hour = "0" + _hour
        else:
            print "Error: invalid params:%s" % param
            print usage
            sys.exit(1)
        i += 2

    if _type is None or _date is None \
       or (_hour is None and _type == 'm2h'):
        print "Error: invalid params"
        print usage
        sys.exit(1)
   
    error_log("start")
   
    fromTableName = "t_m1"
    toTableName = "t_h1"
    if _type == 'h2d':
        fromTableName = "t_h1"
        toTableName = "t_d1"

    fromTable = JKHBase.getTable(fromTableName)
    toTable = JKHBase.getTable(toTableName)

    startTime = None
    endTime = None
    jkTime = None

    if _type == 'm2h':
        startTime = time.mktime(time.strptime( "%s%s00" % (_date, _hour), "%Y%m%d%H%M" ))
        endTime = time.mktime(time.strptime( "%s%s59" % (_date, _hour), "%Y%m%d%H%M" ))
        endTime += 60 # 60 second，查询结束值是开区间的
        jkTime = "%s%s0000" % (_date, _hour)
    else: #h2d
        startTime = time.mktime(time.strptime( "%s00" % _date, "%Y%m%d%H" ))
        endTime = time.mktime(time.strptime( "%s23" % _date, "%Y%m%d%H" ))
        endTime += 3600 # 1 hour
        jkTime = "%s000000" % _date

    jkids = read_jkids()

    for jkid in jkids:
        rows = fromTable.read(jkid, None, startTime, endTime)
        #return: [{jkid:12345, keyTime:201206141130, time:20120614113001, columns:{key1:value1, key2:value2}},...] 

        #{key:[value_sum,value_count],key2:[...]...}
        mergeMap = {}

        #从分钟到小时的汇总
        for row in rows:
            #print row
            for k in row['columns']:
                #不处理可用性信息
                if k == 'status' or k == 'servicedowntime' or k == 'notifications_enabled':
                    continue

                if k not in mergeMap:
                    mergeMap[k] = [0.0, 0]

                mergeMap[k][0] += float(row['columns'][k])
                mergeMap[k][1] += 1

        if len(mergeMap) == 0 : continue
        #print mergeMap
        #merge ok
        line = "jkid=%s`time=%s|" % (jkid, jkTime)
        isFirst = True
        for k in mergeMap:
            if isFirst:
                isFirst = False
            else:
                line += "`"
            average = mergeMap[k][0]/mergeMap[k][1]            
            line += "%s=%f" % (k, average)

        #print "line=", line
        toTable.write(line)
    
    #~for jkids
    error_log("end")

if __name__ == "__main__":
    main()
