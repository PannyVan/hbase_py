#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-09

__doc__ = """
汇总可用性
====================
仅处理可用性数据，监控值数据处理在另一个程序中

汇总数据，从分钟数据汇总成小时数据，或者从小时数据汇
总成日数据。

汇总操作，分钟数据求小时数据的可用性比较复杂
汇总操作，小时数据求天数据的可用性目前仅计算平均值

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

logs_dir = "./logs"

_type = None    #m2h h2d
_date = None    #"20120614"
_hour = None    #"11"

def error_log(msg):
    logFile = "%s/error.log" % logs_dir
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

def computer_ok_percent(rows, startTime, endTime):
    #从分钟到小时的汇总
    first_err_time = None #第一个错误的起始时间
    total_err_time = 0    #累计的全部的错误时间
    
    #错误时间，从第一个错误开始，到后续检查到一个正常结束
    for row in rows:
        if not row['columns'].has_key('status'):
            continue
        
        #仅处理有状态值的
        servicedowntime = row['columns'].get('servicedowntime', 0)
        notifications_enabled = row['columns'].get('notifications_enabled', 1)
        if servicedowntime == 1 or notifications_enabled == 0:
            #特殊字段，表示此时间后已经不需要计算可用性了
            if first_err_time is not None: 
                #结算时间
                curr_status_time = time.mktime(time.strptime("%Y%m%d%H%M%S", row.time))
                total_err_time += curr_status_time - first_err_time
                first_err_time = None
        
        curr_status = row['columns']['status']
        if curr_status == '2': 
            #异常状态
            if row is rows[0]:
                #第一个就是异常状态，把起始时间作为错误开始时间
                first_err_time = startTime
            elif first_err_time is None: 
                first_err_time = time.mktime(time.strptime( row.time, "%Y%m%d%H%M%S" ))
            else:
                #之前已经是异常状态了，不用处理
                pass
        else: 
            #非异常状态，
            if first_err_time is not None : 
                #如果之前是异常状态，可以结算了
                curr_status_time = time.mktime(time.strptime( row.time, "%Y%m%d%H%M%S" ))
                total_err_time += curr_status_time - first_err_time
                first_err_time = None
            else: 
                #之前是非异常状态，不需要处理
                pass
    #~for rows                
    
    if first_err_time is not None: 
        #最后一个是异常，把结束时间当做最后一次结算时间
        curr_status_time = endTime
        total_err_time += curr_status_time - first_err_time
        first_err_time = None
    
    ok_percent = 100 - total_err_time/(endTime - startTime)
    if ok_percent < 0: ok_percent = 0
    return ok_percent

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
        print "Error: invalid params:%s" % param
        print usage
        sys.exit(1)
   
    error_log("start")
   
    fromTableName = "t_m1"
    toTableName = "t_status_h1"
    if _type == 'h2d':
        fromTableName = "t_status_h1"
        toTableName = "t_status_d1"

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

        #可用性
        #监控点处于良好状态时间的百分比
        ok_percent = 0

        if _type == 'm2h':
            #从分钟到小时的汇总
            ok_percent = computer_ok_percent(rows, startTime, endTime)
        else: 
            #h2d 从小时到天的汇总
            sum = 0.0
            count = 0
            for row in rows:
                if row['columns']['status'] is not None:
                    sum += float(row['columns']['status'])
                    count += 1
            average = sum/count
            ok_percent = average

        #merge ok
        line = "jkid=%s`time=%s|" % (jkid, jkTime)
        line += "status=%f" % ok_percent 

        #print "line=", line
        toTable.write(line)
    
    #end for
    error_log("end")

if __name__ == "__main__":
    main()
