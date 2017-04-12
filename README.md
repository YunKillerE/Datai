
## 简单介绍

    基于sqoop封装的抽取程序，封装的目的是配合数据平台展示界面中的数据源管理模块方便获取数据

## 背景

    1，刚开始用shell写的，抽取元数据存储在一个文本文件中，在生产环境运正常行三个月
    2，后来改为用shell+python写的，抽取元数据就存储到mysql中了，仅仅测试通过了，未在生产环境下使用过，如果要使用先自己测试好
    3，再后来直接改为用java写了，抽取元数据也是存储在mysql中，已在生产环境正常运行三个多月了


## 抽取元数据存储在mysql中的表结构

主要有以下几个表：

    1，database_info         存储源服务器地址，账号密码等，包括RDBMS/FTP/FILESYSTEM...
    2，sqoop_info            存储抽取规则，比如增量合并、增量不合并、全量、是否压缩、是否做字段映射、并发等
    3，table_timestamp       存储每个表的抽取时间，主要针对增量抽取
    4，hdfs_export_info      存储从hdfs导出到rdbms中的地址
    5，hdfs_export_opts      存储从hdfs导出到rdbms时的相应参数
    
    表结构，随时都有更新，最新的参照src/sqoop.sql


## 代码结构介绍

    shell目录：
	
        fullv4*.sh  纯shell，使用方法比较简单，所有的参数都在para_file文件中，para_file.xlsx是示例，脚本运行加上表名就可以了
        fullv5.sh   是shell+python版本，所有参数都在mysql中使用方法和上面一样
		
    src目录：
        java版本的抽取，针对上面的版本进行改造，改成了java，使用方法也一样，打成jar包，后面加表名就可以了

## java开发环境

    1，idea
    2，java 1.7
    3，sshxcute 1.0
    4，cdh5.8.4  jars
    
    这里注意：不能采用cdh5.8.4之前的版本，否则sqoop导出会有问题，老版本有bug，SQOOP-2990，cdh5.8.4后修复了

## 目前已实现的功能

    1，从RDBMS中抽取数据到hdfs
    2，把清洗后的数据导出到RDBMS中
    3，从分布式文件系统HDFS中抽取数据，目前仅考虑了非结构化数据，当然把结构化当作非结构化来抽取也是可以的
    4，从本地文件系统中抽取数据，目前仅考虑了非结构化数据，当然把结构化当作非结构化来抽取也是可以的
    5，对hdfs上的数据进行压缩

## 后续规划

    1，计划将datax合并进来，弥补上面结构化的数据的抽取的缺失
    2，增加从RDBMS中直接抽取到hbase中的支持
    3，合并另外一个项目HbaseBulkLoad到当前项目中，实现从hdfs导入数据到hbase中
    4，增加导入hbase后，采用solr进行二级索引的功能
