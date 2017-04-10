# nova_ETL

简单介绍：

    基于sqoop封装的抽取程序，封装的目的是配合数据平台展示界面中的数据源管理模块方便获取数据

背景：

    刚开始用shell写的，后来改为用shell+python写的，此时抽取元数据就存储到mysql中了，再后来直接改为用java写了

抽取元数据存储在mysql中，主要有以下几个表：

   	 1，database_info         存储源服务器地址，账号密码等，包括RDBMS/FTP/FILESYSTEM...
   	 2，sqoop_info            存储抽取规则，比如增量合并、增量不合并、全量、是否压缩、是否做字段映射、并发等
     3，table_timestamp       存储每个表的抽取时间，主要针对增量抽取
   	 4，hdfs_export_info      存储从hdfs导出到rdbms中的地址
  	 5，hdfs_export_opts      存储从hdfs导出到rdbms时的相应参数
	

代码结构介绍：

    shell目录：
	
        fullv4*.sh  纯shell，使用方法比较简单，所有的参数都在para_file文件中，para_file.xlsx是示例，脚本运行加上表名就可以了
        fullv5.sh   是shell+python版本，所有参数都在mysql中使用方法和上面一样
		
    src目录：
        java版本的抽取，针对上面的版本进行改造，改成了java，使用方法也一样，打成jar包，后面加表名就可以了

java开发环境：

    1，idea
    2，java 1.7
    3，sshxcute 1.0
    4，cdh5.8.4  jars
