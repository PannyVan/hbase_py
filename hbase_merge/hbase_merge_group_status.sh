#!/bin/sh
#run date hour

type=${1:-"h"}

if [ $type == "h" ]; then
    dateStr=`date -d "1 hour ago" +"%Y%m%d"`
    hourStr=`date -d "1 hour ago" +"%H"`
    cmd="hbase_merge_group_status.py -type h -date $dateStr -hour $hourStr"
else
    dateStr=`date -d "1 day ago" +"%Y%m%d"`
    cmd="hbase_merge_group_status.py -type d -date $dateStr"
fi

echo "$cmd"
$cmd
