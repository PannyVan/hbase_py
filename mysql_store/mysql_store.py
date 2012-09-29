#!/usr/bin/env python
# -*- coding: utf-8 -*-
# zhaigy@ucweb.com
# 2010-07

import sys
sys.path.append("../jk_hbase_api")

import time
import os

import MySQLdb
from JKHBase import JKHBase

import traceback
"""
从一个特定目录读取文件，文件内容，每行一个，入库。

本程序设计为运行持续一个给定时间后，结束程序。目的是
为了方便从系统定时器重复调用，较短的程序运行时间可以
降低可能存在的内存和网络问题。如果无异常，本程序至少
会完整处理完一个文件才会退出。

输入参数：
-flagId:1    -- 标记一个实例，程序会把pid写入此文件
-runTime:60  -- 运行时间，以秒为单位

文件会先被添加后缀标记，然后才会被处理，如果处理过程
中发生异常，这个带后缀的文件会被留下，问题修复后，简
单更名去掉后缀标记，重新运行即可

2012-09-10 和朱文飞沟通后，修改入口文件格式，示例如下：
jkid=21`time=20120910121005`command_name=notify-service-by-email`command_args=(null)`state=2`host_name=app106`service_description=check cpu`contact_name=liangzw`email=liangzw@ucweb.com`pager=13657654556`output=cpu1=61 cpu2=49
"""

_data_dir = "./data"
_pids_dir = "./pids"
_logs_dir = "./logs"

_flagId = "1"
_runTime = 60

_debug_ = True
def debug(*msg):
    if not _debug_: return
    print msg

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

def mysql_conn():
    conn = MySQLdb.Connection(host='platform32', port=23306, user='root', passwd='root', charset='utf8')
    conn.select_db('jkerror')
    return conn

def parseLine(line):
    kvs = line.split('`')
    kvMap = {}
    for kv in kvs:
        print kv
        (k, v) = kv.split('=', 1)
        kvMap[k]=v
    return kvMap

def work():
    #主业务
    global _data_dir
    global _pids_dir
    global _logs_dir
    
    global _flagId
    global _runTime

    startTime = time.time()
    endTime = startTime + _runTime
    debug( "startTime=%d, endTime=%d" % (startTime, endTime) )

    suffix = "%s" % _flagId
  
    #for mysql cursor
    conn = None
    cursor = None

    try:
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

                #更名为临时文件
                tmpfile = "%s.%s" % (file, suffix)

                try:
                    debug( file, "rename to", tmpfile) #debug
                    os.rename(file, tmpfile)
                except IOError:
                    pass

                if not os.path.exists(tmpfile): continue #lost it

                if cursor is None:
                    debug( "connect mysql table" )
                    conn = mysql_conn()
                    cursor = conn.cursor()

                dealFileNum += 1
                f = open(tmpfile)

                try:
                    #文件逐行处理
                    for line in f:
                        line = line.strip()
                        if len(line) <= 0: continue

                        debug( "line=[%s]" % line )
                        #parser line
                        #jkid=21`time=20120910121005`command_name=notify-service-by-email`
                        #command_args=(null)`state=2`host_name=app106`service_description=check cpu`
                        #contact_name=liangzw`email=liangzw@ucweb.com`pager=13657654556`
                        #output=cpu1=61 cpu2=49
                        kvMap = parseLine(line)
                        debug( "line2=[%s]" % line )
                        jkid = kvMap['jkid']
                        jktime = kvMap['time']
                        jktime = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(jktime, "%Y%m%d%H%M%S"))
                        if not kvMap.has_key('output'):
                            debug("output is empty")
                            continue
                        output = MySQLdb.escape_string(kvMap['output'])
                        if len(output) <= 0:
                            debug("output is empty")
                            continue
                        command_name = kvMap['command_name']
                        command_args = kvMap['command_args']
                        state = kvMap['state']
                        host_name = kvMap['host_name']
                        service_description = kvMap['service_description']
                        contact_name = kvMap['contact_name']
                        email = kvMap['email']
                        pager = kvMap['pager']
                        
                        #indb
                        cursor.execute("insert into t_error(jkid, errdt, command_name, command_args, state, \
                                       host_name, service_description, contact_name, email, pager, output) \
                                       values('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" \
                                       % (jkid, jktime, command_name, command_args, state, host_name, \
                                          service_description, contact_name, email, pager, output))

                    f.close()
                    os.remove(tmpfile) #only have not any error

                except IOError, e:
                    exstr = traceback.format_exc()
                    #error_log(str(e)) 
                    error_log(exstr) 
                    break

                f.close()
                conn.commit()
                debug( "deal %s ok" % file )

                now = time.time() #for
                if now >= endTime: break

            now = time.time() #while
            debug( "now=%d" % now )

            if now >= endTime: break

            if dealFileNum <= 0:
                debug( "sleep 10 ..." )
                time.sleep(10)
                continue
        
        #end while

    except Exception, e:
        error_log(str(e)) 
        exstr = traceback.format_exc()
        print exstr 
    
    except:
        error_log("unknown error!") 
     
    if cursor is not None: cursor.close()
    if conn is not None: conn.close()

def main():
    global _data_dir
    global _pids_dir
    global _logs_dir
    
    global _flagId
    global _runTime

    usage = "usage: mysql_store.py -flagId 1 -runTime 60"
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
    f.close
   
    #----------------------------------------
    try:
        work()
    except Exception, e:
        error_log(str(e))
    except:
        error_log("unknown error!")
    #----------------------------------------

    os.remove(pidFile)
    error_log("end")

if __name__ == "__main__":
    main()
