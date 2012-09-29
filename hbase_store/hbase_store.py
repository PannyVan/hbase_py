#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-07

__doc__="""
数据入库程序
=====================
从一个特定目录读取文件，文件内容，每行一个，入库。

本程序设计为运行持续一个给定时间后，结束程序。目的是
为了方便从系统定时器重复调用，较短的程序运行时间可以
降低可能存在的内存和网络问题。如果无异常，本程序会处
理完目前的全部文件才会退出。

输入参数：
-flagId:1    -- 标记一个实例，程序会把pid写入此文件
-runTime:60  -- 运行时间，以秒为单位，最短运行时间

文件会先被添加后缀标记，然后才会被处理，如果处理过程
中发生异常，这个带后缀的文件会被留下，问题修复后，简
单更名去掉后缀标记，重新运行即可
"""

import sys
sys.path.append("../jk_hbase_api")

import time
import os

from JKHBase import JKHBase

_data_dir = "./data"
_pids_dir = "./pids"
_logs_dir = "./logs"

_flagId = "1"
_runTime = 60

_new_jkids = []
_jkids = None

_debug_ = True
def debug(*msg):
    if not _debug_: return
    print msg

def load_jkids():
    global _jkids
    global _logs_dir
    global _flagId

    debug( "load_jkids" )
    jkidFile = "%s/%s.jkid.list.txt" % (_logs_dir, _flagId)
    _jkids = set()
    debug( "jkidFile:%s" % jkidFile )
    if os.path.exists(jkidFile):
        f = open(jkidFile, "r")
        for line in f:
            line=line.strip('\n')
            if len(line) <=0: continue
            _jkids.add(line)
            debug(line)
        f.close()
    else:
        error_log( "jkidFile:%s is not exits" % jkidFile )
    debug( "jkids size:", len(_jkids) )

def write_new_jkids():
    global _new_jkids
    global _logs_dir
    global _flagId
    if len(_new_jkids) <= 0: return

    global _logs_dir
    jkidFile = "%s/%s.jkid.list.txt" % (_logs_dir, _flagId)
    f = open(jkidFile,"a+")
    for jkid in _new_jkids:
        f.write(jkid)
        f.write('\n')
    f.close()
    _new_jkids = []

_jkid_table = None
def save_jkid(jkid):
    global _jkid_table
    if _jkid_table is None:
        _jkid_table = JKHBase.getTable("t_jkid")

    timeStr = time.strftime("%Y%m%d%H%M%S", time.localtime())
    _jkid_table.write("jkid=%s`time=%s|jkid=jkid" % (timeStr,jkid))
    debug( "save jkid:%s" % jkid )

def error_log(msg):
    global _flagId
    global _logs_dir
    
    logFile = "%s/%s.error.log" % (_logs_dir, _flagId)
    f = open(logFile,"a+")
    timeStr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = "%s: %s" % (timeStr, msg)
    debug( line )
    f.write(line)
    f.write('\n')
    f.close()

def deal_file(tmpfile, table):
    global _jkids
    global _new_jkids
    debug( "deal file %s" % tmpfile )
    f = open(tmpfile)
    try:
        for line in f:
            line = line.strip()
            if len(line) <= 0: continue
            debug( "line=%s" % line )
            jkid = table.write(line)
            if jkid not in _jkids:
                save_jkid(jkid)
                _new_jkids.append(jkid)
                _jkids.add(jkid)

        f.close()
        os.remove(tmpfile) #only have not any error
    except Exception, e:
        error_log("deal_file Error:" + str(e))
    except:
        error_log("deal_file unknown error!")
    f.close()
    debug( "deal file %s ok" % tmpfile )

def work():
    global _data_dir
    global _pids_dir
    global _logs_dir
    
    global _flagId
    global _runTime

    global _jkids
    global _new_jkids

    startTime = time.time()
    endTime = startTime + _runTime
    debug( "startTime=%d, endTime=%d" % (startTime, endTime) )

    suffix = "%s" % _flagId
    table = None #for habase table
    debug( "while True" )
    while True:
        files = os.listdir(_data_dir)
        debug( "file num=%d" % len(files) )
        dealFileNum = 0
        for file in files:
            file = "%s/%s" % (_data_dir, file)
            debug( "file=%s" % file )
            if not os.path.isfile(file): 
                debug( "not is file" )
                continue
            if not file.endswith(".log"): 
                debug( "not end with .log" )
                continue
            tmpfile = "%s.%s" % (file, suffix)
            try:
                debug( file, "rename to", tmpfile )
                os.rename(file, tmpfile)
            except IOError:
                pass
            if not os.path.exists(tmpfile): continue #lost it
            
            if table is None:
                debug( "connect hbase table" )
                table = JKHBase.getTable("t_m1")
            if _jkids is None:
                load_jkids()

            dealFileNum += 1
            #-------------------
            deal_file(tmpfile, table)
            #-------------------
        #~for file
        if dealFileNum > 0:continue
        #在一次循环中没有实际处理文件才允许退出
        now = time.time()
        debug( "now=%d" % now )
        #没有数据，并且时间到了
        if now >= endTime: break
        #时间没有到，休息
        debug( "sleep 10 ..." )
        time.sleep(10)
    #~while True

def main():
    global _pids_dir
    
    global _flagId
    global _runTime

    usage = "usage: hbase_store.py -flagId 1 -runTime 60"
    argvLen = len(sys.argv)
    if argvLen != 5:
        print "argvLen=%d" % argvLen
        print sys.argv
        print usage
        sys.exit(1)

    i = 1
    while i < argvLen:
        param = sys.argv[i]
        if param == '-flagId':
            _flagId = sys.argv[i+1]
        elif param == '-runTime':
            _runTime = int(sys.argv[i+1])
        else:
            print "Error: invalid params:%s" % param
            print usage
            sys.exit(1)
        i += 2
   
    error_log("start")
    pidFile = "%s/%s" % (_pids_dir, _flagId)
    if os.path.isfile(pidFile):
        error_log("pid file[%s] is exists!" % pidFile)
        sys.exit(1)
    
    debug( "write pidfile" )
    pid = os.getpid()
    f = open(pidFile,"w+")
    f.write("%d" % pid)
    f.close()
    
    #---------------------------------- 
    try:
        work()
    except Exception, e:
        error_log("Error:" + str(e))
    except:
        error_log("unknown error!")
    
    try:
        write_new_jkids()
    except Exception, e:
        error_log("Error:" + str(e))
    except:
        error_log("unknown error!")
    #---------------------------------- 

    os.remove(pidFile)
    error_log("end")

if __name__ == "__main__":
    main()
