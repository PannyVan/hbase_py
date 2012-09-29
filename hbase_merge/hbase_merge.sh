#!/bin/sh
#run date hour

values=${1:-"values"}
type=${2:-"m2h"}

if [ $type == "m2h" ]; then
    dateStr=`date -d "1 hour ago" +"%Y%m%d"`
    hourStr=`date -d "1 hour ago" +"%H"`
    cmd="hbase_merge_${values}.py -type m2h -date $dateStr -hour $hourStr"
else
    dateStr=`date -d "1 day ago" +"%Y%m%d"`
    cmd="hbase_merge_${values}.py -type h2d -date $dateStr"
fi

echo "$cmd"
$cmd
