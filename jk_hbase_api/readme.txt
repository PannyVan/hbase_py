#-- utf-8 --
#前提条件：
#1. 需要先安装python的thrift组件
#2. 需要一个当前路径下的配置文件：hbase.host.txt 来指定HBase的服务器
#简单应用举例：（更详细的实例参考TestJKHTable.py）
table=JKHBase.getTable("t_m1")
table.write("jkid=12345`time=20120614113001|key1=value1`key2=value2")
rs=table.read("12345",None,0,time.time())

得到的数据格式如下：
[{jkid:12345, keyTime:201206141130, time:20120614113001, columns:{key1:value1, key2:value2}},...]

说明：
外层是数组，数组的元素是Map，一个Map代表一行记录
Map的主要key有：jkid和columns，这层的key是写死的，不会变
    columns对应的value也是Map，此Map代表多个列和值，key是列名，value是值，具体的列名和个数可能会根据实际输入而不同。

