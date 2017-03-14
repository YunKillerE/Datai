set -x
#!/bin/bash

#Auther:云尘

#全局参数
unset ROOT_DIRECTORY

ROOT_DIRECTORY="/ods" #源数据的hdfs根目录存储路径
TODAY_TIME=`date +%Y%m%d` #当天日期
YESTDAY_TIME=`date -d"yesterday" +"%Y%m%d"` #昨天的日期
#MAP_COUNT="1" #map任务个数，也是生成文件的个数
PARA_PATH="/opt/para_path" #本地变量根目录
PARA_FILE="$PARA_PATH/para_file" #数据库相关信息

HDFS_ADDRESS="hdfs://dser2"
PARA_HIVE="$PARA_PATH/hivesql"
HIVE_DATABASE_NAME="ods"

#shell的环境测试
function shell_check(){

	if [ -d $PARA_PATH ];then
		:
	else
		echo "$PARA_PATH不存在，表信息找不到"
		exit 1
	fi

	if [ -d $PARA_HIVE ];then
		:
	else
		mkdir $PARA_HIVE
	fi
	
	if [ -s $PARA_FILE ];then
		:
	else
		echo "$PARA_FILE不存在或者为空"
		exit 1
	fi
	
	#if [ X$# != 1  ];then
	#	echo "参数错误，请传入表名。example：./sqoop.sh table_name"
	#	exit 1
	#fi
	if [ -d $PARA_PATH/time ];then
		:
	else
		mkdir $PARA_PATH/time
		ls $PARA_PATH
	fi
	 
}


function sqoop_check_first(){
	DATEBASE_TYPE=`echo $1 | cut -d':' -f2`
	echo DATEBASE_TYPE=$DATEBASE_TYPE

	if [ X$DATEBASE_TYPE = Xmysql ] ; then
		CHECK_SQL="select * from $4 limit 10" #mysql sql
	elif [ X$DATEBASE_TYPE = Xoracle ] ; then
		CHECK_SQL="select * from $4 where rownum < 10" #oracle sql
	else
		echo "sqoop_check_first datebase type error!! or table_name error!!"
		exit 1
	fi

	sqoop eval --connect $1 --username $2 --password $3 --query "$CHECK_SQL"

	if [ X$? = X0 ];then
		#echo "数据库连接测试成功！！"
		echo -e "\033[41;37m $4  数据库连接测试成功！！ \033[0m"
		echo -e "\033[41;37m $4  数据库连接测试成功！！ \033[0m" >> $PARA_PATH/sucess
	else
		echo "数据库连接测试失败，请检查jdbc/账号/密码/表名登是否存在"
		echo -e "\033[41;37m $4  数据库连接失败！！ \033[0m" >> $PARA_PATH/error
		exit 1
	fi

}

function sqoop_full(){

	if [ $1 ];then
		hdfs dfs -rm -r -f $ROOT_DIRECTORY/$4/"$TODAY_TIME"_full
	fi

	#sqoop-import --connect jdbc:mysql://cdh:3306/dmp --username root --password hadoop --table dmp_tag -m 1 --target-dir /user/root/mm2 -z --compression-codec lzo
	
	#记录时间戳的最大值
	TIME=`sqoop eval --connect $1 --username $2 --password $3 --query "select max($7) from $4"`
	tmp_timestamp=`echo $TIME |awk -F'|' '{print $4}'`
	echo $tmp_timestamp
	echo $TIME

	echo $tmp_timestamp >> $PARA_PATH/time/"$4".lastmaxtime
	
	if [ X$5 = Xno -a X$6 = Xno ];then
		sqoop import --connect $1 --username $2 --password $3 --table $4 -m $MAP_COUNT --target-dir $ROOT_DIRECTORY/$4/"$TODAY_TIME"_full --null-non-string '\\N' --null-string '\\N' --fields-terminated-by '\001' --hive-drop-import-delims $map_columns
	elif [ X$5 != Xno -a X$6 = Xno ];then                                    
		sqoop import --connect $1 --username $2 --password $3 --table $4 -m $MAP_COUNT --target-dir $ROOT_DIRECTORY/$4/"$TODAY_TIME"_full -z --compression-codec $5 --null-non-string '\\N' --null-string '\\N' --fields-terminated-by '\001' --hive-drop-import-delims $map_columns
	elif [ X$5 != Xno -a X$6 != Xno ];then                                   
		sqoop import --connect $1 --username $2 --password $3 --table $4 -m $MAP_COUNT --target-dir $ROOT_DIRECTORY/$4/"$TODAY_TIME"_full -z --compression-codec $5 --as-"$6" --null-non-string '\\N' --null-string '\\N' --fields-terminated-by '\001' --hive-drop-import-delims $map_columns
	else
		echo "compress or parquert args input error"
		exit 1
	fi
	
}

function sqoop_delta(){

	if [ $1 ];then
		hdfs dfs -rm -r -f $ROOT_DIRECTORY/$4/"$TODAY_TIME"_delta
	fi


        #记录时间戳的最大值
        TIME=`sqoop eval --connect $1 --username $2 --password $3 --query "select max($7) from $4"`
        tmp_timestamp="`echo $TIME |awk -F'|' '{print $4}'`"
        echo $tmp_timestamp
        echo $TIME

        echo $tmp_timestamp >> $PARA_PATH/time/"$4".lastmaxtime
	
	t1=`echo $8 |cut -d'"' -f2`
	t2=`echo $tmp_timestamp|cut -d' ' -f1-2`
	if [ X$TIME_VAR = Xyes ];then
	echo t1====$t1
	echo t2====$t2
	#sh /opt/para_path/yy.sh "$t1" "$t2"
		sqoop-import --connect $1 --username $2 --password $3 --query "SELECT * FROM $4 where $7 > '$t1' and $7 <= '$t2' and \$CONDITIONS" -m $MAP_COUNT --target-dir $ROOT_DIRECTORY/$4/"$TODAY_TIME"_delta --null-non-string '\\N' --null-string '\\N' --fields-terminated-by '\001' --hive-drop-import-delims $map_columns --split-by $9
	elif [ X$5 = Xno -a X$6 = Xno -a X$10 = no ];then
		sqoop-import --connect $1 --username $2 --password $3 --table $4 -m $MAP_COUNT --target-dir $ROOT_DIRECTORY/$4/"$TODAY_TIME"_delta --check-column $7 --incremental lastmodified --last-value "$8" --null-non-string '\\N' --null-string '\\N' --fields-terminated-by '\001' --hive-drop-import-delims $map_columns
	elif [ X$5 != Xno -a X$6 = Xno -a X$10 = no ];then                                    
		sqoop-import --connect $1 --username $2 --password $3 --table $4 -m $MAP_COUNT --target-dir $ROOT_DIRECTORY/$4/"$TODAY_TIME"_delta -z --compression-codec $5 --check-column $7 --incremental lastmodified --last-value "$8" --null-non-string '\\N' --null-string '\\N' --fields-terminated-by '\001' --hive-drop-import-delims $map_columns
	elif [ X$5 != Xno -a X$6 != Xno -a X$10 = no ];then                                   
		sqoop-import --connect $1 --username $2 --password $3 --table $4 -m $MAP_COUNT --target-dir $ROOT_DIRECTORY/$4/"$TODAY_TIME"_delta -z --compression-codec $5 --as-"$6" --check-column $7 --incremental lastmodified --last-value "$8" --null-non-string '\\N' --null-string '\\N' --fields-terminated-by '\001' --hive-drop-import-delims $map_columns
	else
		echo "delta args input error"
		exit 1
	fi
 
    sqoop_merge $1 $2 $3 $4 $7 $9
	echo $tmp_timestamp >> $PARA_PATH/time/"$4".lastmaxtime

}

function sqoop_merge(){

	if [ $1 ];then
		hdfs dfs -rm -r -f $ROOT_DIRECTORY/$4/"$TODAY_TIME"_full
	fi

	#1,生成相应jar包
	sqoop codegen --connect $1 --username $2 --password $3 --table $4 --bindir $PARA_PATH/$4/ --input-null-non-string '\\N' --input-null-string '\\N' --input-fields-terminated-by '\001' --null-non-string '\\N' --null-string '\\N' --fields-terminated-by '\001' $map_columns
echo "sqoop codegen --connect $1 --username $2 --password $3 --table $4 --bindir $PARA_PATH/$4/"
	
	#2，将今天的抽取内容和昨天的全量进行合并
	if hdfs dfs -ls $ROOT_DIRECTORY/$4/"$TODAY_TIME"_delta ;then
		:
	else
		echo "$ROOT_DIRECTORY/$4/"$TODAY_TIME"_delta is not exist!!"
		exit 1
	fi
	
	if hdfs dfs -ls $ROOT_DIRECTORY/$4/"$YESTDAY_TIME"_full ;then
		:
	else
		echo "$ROOT_DIRECTORY/$4/"$YESTDAY_TIME"_delta is not exist!!"
		exit 1
	fi
	#class_name=`echo $4 |tr A-Z a-z |cut -d'.' -f2`	
	class_name1=`echo $4 |cut -d'.' -f1`	
	class_name2=`echo $4 |cut -d'.' -f2`
	class_name="$class_name1"_"$class_name2"
	sqoop merge --new-data $ROOT_DIRECTORY/$4/"$TODAY_TIME"_delta --onto $ROOT_DIRECTORY/$4/"$YESTDAY_TIME"_full --target-dir $ROOT_DIRECTORY/$4/"$TODAY_TIME"_full --jar-file $PARA_PATH/$4/"$4".jar --class-name $class_name --merge-key $6
echo "sqoop merge --new-data $ROOT_DIRECTORY/$4/"$TODAY_TIME"_delta --onto $ROOT_DIRECTORY/$4/"$YESTDAY_TIME"_full --target-dir $ROOT_DIRECTORY/$4/"$TODAY_TIME"_full --jar-file $PARA_PATH/$4/"$4".jar --class-name $class_name --merge-key $6"
	
	
}

#主函数
#	\$1:		:增量或者全量
function main(){

	sqoop_check_first $jdbc_link $username $password $table_name
	
	sqoop_sql $jdbc_link $username $password $table_name

	if [ X$1 = Xfull ];then
	
		sqoop_full $jdbc_link $username $password $table_name $compress $parquert $timestamp
		
	elif [ X$1 = Xdelta ];then
	
		sqoop_delta $jdbc_link $username $password $table_name $compress $parquert $timestamp "$lastmaxtime" $PRI_KEY $TIME_VAR
		
	else
		echo "input error! only full or delta!!"
		exit 1
	fi

#数据库名，表名
#       \$1:    $HIVE_DATABASE_NAME
#       \$2:    $table_name
#       \$3:    $TODAY_TIME
#       \$4：   $full_incre

	add_hive_partition $HIVE_DATABASE_NAME $table_name $TODAY_TIME $full_incre
	
}


function begin_full_delta(){

	if [ -z "$lastmaxtime" ];then
		if [ X$full_delta = Xdelta ];then
			echo "时间戳不存在不能进行增量抽取"
			exit 1
		else
			main $full_delta
		fi
	else
		main $full_delta 
	fi

}

#sqoop_sql $jdbc_link $username $password $table_name
function sqoop_sql(){

	CHECK_SQL="select count(*) from $4"

	COUNT_TMP=`sqoop eval --connect $1 --username $2 --password $3 --query "$CHECK_SQL"`
	COUNT=`echo $COUNT_TMP |awk -F'|' '{print $4}'`
	echo $COUNT_TMP
	echo $COUNT

        TIME=`sqoop eval --connect $1 --username $2 --password $3 --query "select max($timestamp) from $4"`
        tmp_timestamp="`echo $TIME |awk -F'|' '{print $4}'`"
        echo $tmp_timestamp
        echo $TIME

	
	echo $table_name $COUNT "$tmp_timestamp" >> $PARA_PATH/"$TODAY_TIME"_count
	

}


#数据库名，表名
#	\$1:	$HIVE_DATABASE_NAME
#	\$2:	$table_name
#	\$3:	$TODAY_TIME
#	\$4：	$full_incre
function add_hive_partition(){
	
	#1，生成sql文件
	#2，执行sql文件
	
	table_name1=`echo $2 | cut -d'.' -f1`
	table_name2=`echo $2 | cut -d'.' -f2`
	table_name3="$table_name1"_"$table_name2"
	
	if [ X$full_delta = Xfull ];then
	
		echo alter table "$table_name3"_full drop partition \(dt=\"$3\"\)\; > $PARA_HIVE/$2
		echo alter table "$table_name3"_full add partition \(dt=\""$3"\"\) location \"$HDFS_ADDRESS/$ROOT_DIRECTORY/$2/"$3"_full\"\; >> $PARA_HIVE/$2
		echo "exit;" >> $PARA_HIVE/$2
		cat $PARA_HIVE/$2
		hive --database $1 -i $PARA_HIVE/$2
		
	elif [ X$full_delta = Xdelta ];then
	
		echo alter table "$table_name3"_full drop partition \(dt=\"$3\"\)\; > $PARA_HIVE/$2
		echo alter table "$table_name3"_delta drop partition \(dt=\"$3\"\)\; >> $PARA_HIVE/$2
		echo alter table "$table_name3"_full add partition \(dt=\"$3\"\) location \"$HDFS_ADDRESS/$ROOT_DIRECTORY/$2/"$3"_full\"\; >> $PARA_HIVE/$2
		echo alter table "$table_name3"_delta add partition \(dt=\"$3\"\) location \"$HDFS_ADDRESS/$ROOT_DIRECTORY/$2/"$3"_delta\"\; >> $PARA_HIVE/$2
		echo "exit;" >> $PARA_HIVE/$2
		cat $PARA_HIVE/$2
		hive --database $1 -i $PARA_HIVE/$2
		
	else
	
		echo "$full_delta is error!!"
		exit 1
	fi
	
	if [ X$? = X0 ];then
		echo "$2 $3 load to hive sucess!!" >> $PARA_PATH/hive_sucess
	else
		echo "$2 $3 load to hive error!!" >> $PARA_PATH/hive_error
	fi
}




function variable_get(){

shell_check

#==========================获取相应的变量值=============================================
#获取表的信息
#TABLE_INFO=`cat $PARA_FILE |grep $1`
TABLE_INFO=`python $PARA_PATH/pym.py "select a.database_link,a.database_username,a.database_pwd,b.table_name,b.sqoop_timestamp,b.sqoop_delta_full,b.sqoop_compress_format,b.sqoop_storage_format,b.sqoop_map_count,b.sqoop_pri_key,b.sqoop_time_varchar,b.sqoop_map_column_java from  database_info as a INNER JOIN sqoop_info as b WHERE a.database_id=b.sqoop_id and b.table_name=\"$1\"" s`
#TABLE_INFO="$1"
#根据表信息确定相关变量值
jdbc_link=`echo $TABLE_INFO |awk '{print $1}'`
username=`echo $TABLE_INFO |awk '{print $2}'`
password=`echo $TABLE_INFO |awk '{print $3}'`
table_name=`echo $TABLE_INFO |awk '{print $4}'`
timestamp=`echo $TABLE_INFO |awk '{print $5}'`
full_delta=`echo $TABLE_INFO |awk '{print $6}'`
compress=`echo $TABLE_INFO |awk '{print $7}'`
parquert=`echo $TABLE_INFO |awk '{print $8}'`
MAP_COUNT=`echo $TABLE_INFO |awk '{print $9}'`
PRI_KEY=`echo $TABLE_INFO |awk '{print $10}'`


#上一次最大时间戳,这是增量抽取时记录的值，如果没进行增量抽取这个值将不存在
if [ X$full_delta = Xdelta -a -f $PARA_PATH/time/"$table_name".lastmaxtime ];then
        lastmaxtime=\"`cat $PARA_PATH/time/"$table_name".lastmaxtime |tail -n 1`\"
else
        echo "时间戳不存在，只能进行全量抽取"
fi

#改变字段类型
MAP_COLUME=`echo $TABLE_INFO |awk '{print $12}'`
#if [ -z $MAP_COLUME ];then
if [  X$MAP_COLUME = XNone ];then
	map_columns=""
else
	map_columns="--map-column-java $MAP_COLUME=String"
fi

#时间戳字段为非时间类型
TIME_VAR="`echo $TABLE_INFO |awk '{print $11}'`"


#==========================获取相应的变量值=============================================

echo jdbc_link=$jdbc_link
echo username=$username
echo password=$password
echo table_name=$table_name
echo timestamp=$timestamp
echo full_delta=$full_delta
echo compress=$compress
echo parquert=$parquert
echo lastmaxtime="$lastmaxtime"
echo MAP_COUNT=$MAP_COUNT
echo PRI_KEY=$PRI_KEY
echo MAP_COLUME=$MAP_COLUME
echo map_columns=$map_columns


#sqoop_sql $jdbc_link $username $password $table_name

#sqoop_check_first $jdbc_link $username $password $table_name

begin_full_delta

}


#for i in `cat $PARA_FILE`
#do
#	echo "i====\"$i\""
#	variable_get "$i"
#done

variable_get $1

set +x

