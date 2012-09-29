#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-09

__doc__ = """
业务分组的可用性汇总
====================
业务分组下辖多个jkid，这里汇总同一个时间级别内其全部
下辖jkid的可用性平均值。

本程序设计为定时启动运行。
本程序可以使用不同参数并发运行，但是即便是相同参数也
不会有不良影响。

输入参数：
-type:h/d       -- 小时数据，日数据汇总
-date:20120614  -- 要统计的日期
-hour:11        -- 要统计的小时，仅对type=h有效

重新运行可以覆盖旧值
"""

import sys
sys.path.append("../jk_hbase_api")

import time
import os

import urllib2
import simplejson

from JKHBase import JKHBase

logs_dir = "./logs"

_type = None    #h/d
_date = None    #"20120614"
_hour = None    #"11"

_inface_host = "http://dev6.ucweb.local:9981"

#---------------------------
#开关
#---------------------------
_proxy="192.168.59.175:80"
#_proxy=None

def debug(msg):
    #print msg
    pass
#---------------------------

def error_log(msg):
    logFile = "%s/error.log" % logs_dir
    f = open(logFile,"a+")
    timeStr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = "%s: %s" % (timeStr, msg)
    print line
    f.write(line)
    f.write('\n')
    f.close()

# 业务分组id，树结构
# http://dev6.ucweb.local:9981/api/category/list/
def read_gpids():
    #gpid_tree={'A':['B','C'],'B':[],'C':[]}
    gpid_tree={}
    r = urllib2.Request("%s/api/category/list/" % _inface_host)
    if _proxy is not None and _proxy != "":
        r.set_proxy(_proxy,"http")
    f = urllib2.urlopen(r)
    #{    
    #    "result": {        
    #        "<业务分类ID>": {            
    #            "is_leaf": 是否为叶子,            
    #            "pid": "<父亲分类>"        
    #        }    
    #    },    
    #    "success": true
    #}
    json_obj = simplejson.load(f)
    if json_obj['success']:
        for id in json_obj['result']:
            if gpid_tree.has_key(id):
                pass
            else:
                gpid_tree[id]=[]
                is_leaf = json_obj['result'][id]['is_leaf']
                if is_leaf: continue #叶节点直接继续
                pid = json_obj['result'][id]['pid']
                if pid == id: continue #特别的异常数据
                debug( "pid %s" % pid )
                if pid is not None:
                    if gpid_tree.has_key(pid):
                        gpid_tree[pid].append(id)
                    else:
                        gpid_tree[pid]=[id]
    else:
        error_log(json_obj['msg'])

    f.close()
    debug( gpid_tree )
    return gpid_tree

#获取分组下的jkid
#http://dev6.ucweb.local:9981/api/services/by_category/?cid=1060212
#这个的返回结果可能是空结果集
def read_jkids(gpid):
    r = urllib2.Request("%s/api/services/by_category/?cid=%s" % ( _inface_host, gpid))
    if _proxy is not None and _proxy != "":
        r.set_proxy(_proxy,"http")
    f = urllib2.urlopen(r)
    #{
    #      "result": {
    #                    "jkids":  ["<JKID>"...],
    #                    "cid": "<查询的业务分类ID>"
    #                },
    #      "success": true
    #}
    json_obj = simplejson.load(f)
    if json_obj['success']:
        return json_obj['result']['jkids']
    else:
        error_log(json_obj['msg'])

#计算叶节点的平均status值
def compute_leafnode_average_status(rows):
    sum = 0.0
    count = 0
    for row in rows:
        #return: [{jkid:12345, keyTime:201206141130, time:20120614113001, columns:{key1:value1, key2:value2}},...] 
        status = float(row['columns']['status'])
        sum += status
        count += 1
    #~for
    average = sum/count
    return average

#计算并储存gpid对应的可用性数据
#注意，如果叶节点没有下属监控节点，其值是None
_call_cas_numb=0 #避免程序和数据异常造成的死循环
def compute_average_status(gpid_tree,group_status, gpid):
    global _call_cas_numb
    _call_cas_numb += 1
    if _call_cas_numb > 200: 
        error_log("compute_average_status be called > 200")
        sys.exit(1)
    debug( "compute_average_status, gpid:%s" % gpid )
    if group_status.has_key(gpid): return group_status[gpid] 
    error_log( "parent node, gpid=%s" % gpid )
    sum = 0
    count = 0
    for child_gpid in gpid_tree[gpid]:
        status = compute_average_status(gpid_tree, group_status, child_gpid)
        if status is None: continue
        sum += status 
        count += 1
    if count == 0 : #没有下属监控节点
        group_status[gpid] = None
    else:
        average = sum/count
        group_status[gpid] = average

def main():
    global _type
    global _date
    global _hour
    global _call_cas_numb

    usage = "usage: %s -type h/d -date 20120614 [-hour 11]" % sys.argv[0]
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
       or (_hour is None and _type == 'h'):
        print "Error: invalid params"
        print usage
        sys.exit(1)
   
    error_log("start")
  
    fromTableName = "t_status_h1"
    toTableName = "t_status_h1_group"

    if _type == 'd':
        fromTableName = "t_status_d1"
        toTableName = "t_status_d1_group"

    fromTable = JKHBase.getTable(fromTableName)
    toTable = JKHBase.getTable(toTableName)

    startTime = None
    endTime = None
    keyTime = None

    if _type == 'h':
        startTime = time.mktime(time.strptime( "%s%s00" % (_date, _hour), "%Y%m%d%H%M" ))
        endTime = time.mktime(time.strptime( "%s%s59" % (_date, _hour), "%Y%m%d%H%M" ))
        endTime += 60 # 60 second，查询结束值是开区间的
        jkTime = "%s%s0000" % (_date, _hour)
    else: #d
        startTime = time.mktime(time.strptime( "%s00" % _date, "%Y%m%d%H" ))
        endTime = time.mktime(time.strptime( "%s23" % _date, "%Y%m%d%H" ))
        endTime += 3600 # 1 hour
        jkTime = "%s000000" % _date

    gpid_tree = read_gpids()
    group_status = {}

    #第一次循环先计算叶节点数据
    for gpid in gpid_tree:
        #跳过有子节点的节点
        if len(gpid_tree[gpid]) != 0: continue
        #仅计算叶节点
        error_log( "leaf node, gpid=%s" % gpid )
        jkids = read_jkids(gpid)
        debug (jkids)
        all = []
        for jkid in jkids:
            #读的是特定时间的数据
            rows = fromTable.read(jkid, None, startTime, endTime)
            #实际应该是最多一条数据
            if len(rows) > 1:
                error_log("read data error [%s]" % startTime)
                error_log( rows )
                sys.exit(1)
            elif len(rows) == 0:
                continue #空，没有数据
            else:
                all.append(rows[0])
        #~for
        if len(all) > 0:
            group_status[gpid]=compute_leafnode_average_status(all)
        else:
            #有可能没有下属监控点，需要处理，设定一个特殊值，表述：没有下属监控节点
            group_status[gpid]=None

        del all
    #~for

    #第二次循环仅计算父节点
    for gpid in gpid_tree:
        if len(gpid_tree[gpid]) == 0: continue
        if group_status.has_key(gpid): continue
        _call_cas_numb = 0
        compute_average_status(gpid_tree, group_status, gpid)
    #~for
    
    #save
    for gpid in group_status:
        status = group_status[gpid]
        if status is None: continue
        line = "jkid=%s`time=%s|status=%f" % (jkid, jkTime, status)
        #print "line=", line
        toTable.write(line)
    #end for
    error_log("end")

if __name__ == "__main__":
    main()
