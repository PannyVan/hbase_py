#!/bin/sh

values=${1:-"values"}
type=${2:-"m2h"}
date1=${3:-"2012-09-20"}
now=`date -d "+1 day" +"%Y-%m-%d"`
date2=${4:-"$now"}

time1=`date -d $date1 +"%s"`
time2=`date -d $date2 +"%s"`
time3=$(($time2 - $time1))
day=$(((time3+86400-1)/86400))

for ((d=0;d<$day;d++)); do
    dateStr=`date -d "$date1 +$d day" +"%Y%m%d"`
    if [ $type == "m2h" ]; then
        for ((i=0;i<24;i++)); do
            cmd="hbase_merge_${values}.py -type m2h -date $dateStr -hour $i"
            echo "$cmd"
            $cmd
        done
    else
        cmd="hbase_merge_${values}.py -type h2d -date $dateStr"
        echo "$cmd"
        $cmd
    fi
done
