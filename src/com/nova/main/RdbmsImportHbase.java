package com.nova.main;

import com.nova.utils.DBUtils;
import com.nova.utils.PropertiesUtils;
import com.nova.utils.SqoopUtils;
import net.neoremind.sshxcute.exception.TaskExecFailException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yunchen on 2017/4/6.
 */
public class RdbmsImportHbase {

    public static void main(String[] args) throws TaskExecFailException {


        /*String path = "C:\\Users\\yunchen\\Desktop\\yunchen\\Datai\\src\\config.properties";//properties文件位置
        String tableName = "YUNCHEN.GA_CZRKJBXX";//当前需要操作的表名*/
        String path = args[0];
        String tableName = args[1];

        //当前时间
        Date d = new Date();
        Date yd = new Date(d.getTime() - 86400000L);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        SimpleDateFormat sdf4 = new SimpleDateFormat("yyyyMMddhhmmss");

        String date3 = sdf3.format(d);
        String date4 = sdf4.format(d);
        String yesterday = sdf.format(yd);
        String yesterday2 = sdf2.format(yd);

        System.out.println("当前时间：" + sdf.format(d));
        System.out.println("昨天时间：" + yesterday);
        System.out.println("date3-------------->："+date3);
        System.out.println("date4-------------->："+date4);

        //获取mysql元数据库所需的连接的变量
        String username = PropertiesUtils.Get_Properties(path, "username");
        String url = PropertiesUtils.Get_Properties(path, "url");
        String password = PropertiesUtils.Get_Properties(path, "password");
        String selectsql = PropertiesUtils.Get_Properties(path, "selectsql") + "\"" + tableName + "\"";
        String selectlasttime = PropertiesUtils.Get_Properties(path, "selectlasttime") + "\"" + tableName + "\"" ;
        String sqoop_server_ip = PropertiesUtils.Get_Properties(path,"sqoop_server_ip");
        String sqoop_server_user = PropertiesUtils.Get_Properties(path,"sqoop_server_user");


        Map tableMap = DBUtils.get_parafile(url,username, password, selectsql);
        //System.out.println("待写入表"+tableName+"信息如下：");
        System.out.println(tableMap);

        String parafile_url = (String) tableMap.get("database_link");
        String parafile_username = (String) tableMap.get("database_username");
        String parafile_password = (String) tableMap.get("database_pwd");
        String parafile_timestamp = (String) tableMap.get("sqoop_timestamp");//时间戳字段
        String parafile_sqoop_time_varchar = (String) tableMap.get("sqoop_time_varchar");//是否为varchar类型


        //获取最大值并插入数据库
        String sqoop_max;

        sqoop_max = "source /etc/profile;sqoop eval --connect "+ parafile_url
                +" --username "+parafile_username+" --password "+parafile_password+" --query \"select max("+parafile_timestamp+") from "
                +tableName+"\"";


        System.out.println(sqoop_max);
        String max = "";
        try {
            max = SqoopUtils.SelectMaxUseSSH(sqoop_server_ip,sqoop_server_user,sqoop_max);
        } catch (TaskExecFailException e) {
            e.printStackTrace();
        }
        System.out.println("max="+max);
        System.out.println(selectlasttime);
        Map lasttimeMap = DBUtils.get_parafile(url, username, password, selectlasttime);
        String lasttime = (String) lasttimeMap.get("maxtime");
        String insertsql = "UPDATE table_timestamp set maxtime='"+max+"' where table_name = \""+tableName+"\"";
        System.out.println("max="+insertsql);
        DBUtils.insert(url,username,password,insertsql);

        String sqoop_command = "source /etc/profile;sqoop import --connect " + tableMap.get("database_link") + " --table " + tableMap.get("table_name") + " --username " + tableMap.get("database_pwd") +
                " --password " + tableMap.get("database_pwd") + "--check-column SXBZK_RKSJ --incremental lastmodified --last-value '"+ lasttime +"' --columns " +
                "RYNBID,RYID,HHNBID,MLPNBID,ZPID,NBSFZID,GMSFHM,QZJLXKQFJGMC,YXQQSRQ,YXQJZRQ,XM,CYM,XMHYPY,CYM_XMHYPY,XBDM,MZDM,CSRQ,CSSJ," +
                "CSDGJHDQDM,CSDSSXDM,CSD_DZMC,LXDH,JHRY_XM,JHRY_GMSFHM,JHRY_JHGXDM,JHRE_XM,JHRE_GMSFHM,JHRE_JHGXDM,FQ_XM,FQ_GMSFHM,MQ_XM," +
                "MQ_GMSFHM,PO_XM,PO_GMSFHM,JGGJDQDM,JGSSXDM,ZJXYDM,XLDM,HYZKDM,BYZKDM,SG,XXDM,ZY,ZYLB,FWCS,XXJB,QL_RQ,HYQL,QL_GJHDQDM,QL_XZQHDM," +
                "QL_DZMC,LBZ_RQ,HYLBZ,LBZ_GJHDQDM,LBZ_XZQHDM,LBZ_DZMC,SWRQ,SWZXLB,SW_ZXSJ,QLCRQ,QCZXLB,QWD_GJHDQDM,QWD_XZQHDM,QWD_DZMC,CSZMBH," +
                "CSZ_QFRQ,HYLB,QT_XZQHDM,QT_DZMC,RYLB,HB,YHZGXDM,RYZT,RYSDZT,LXDBID,BZ,JLBZ_PDBZ,YWNR,CJHJYWID,CCHJYWID,QY_RQSJ,JSSJ,CXBZ,ZJLB," +
                "JLX,MLPH,MLXXDZ,PCS,ZRQ,XZJD,JCWH,PXH,MLPID,XZQHDM,HH,HLX,HHID,BDFW,XXQY_RQSJ,LXDH02,XZDCJZT,ZWYZW,ZWYZCJG,ZWEZW,ZWEZCJG,ZWCJJGDM," +
                "SZYCZKDM,X,M,JG_DZMC,JHRY_CYZJDM,JHRY_ZJHM,JHRY_WWX,JHRY_WWM,JHRY_LXDH,JHRE_CYZJDM,JHRE_ZJHM,JHRE_WWX,JHRE_WWM,JHRE_LXDH," +
                "FQ_CYZJDM,FQ_ZJHM,FQ_WWX,FQ_WWM,MQ_CYZJDM,MQ_ZJHM,MQ_WWX,MQ_WWM,PO_CYZJDM,PO_ZJHM,PO_WWX,PO_WWM,CYZKDWBM,CYZK_DWMC,HQYLDYY,SWYY," +
                "QCQYLDYY,ZXSJ,GXSJ,SJGSDWDM,SJGSDWMC,HJD_DZBM,HJD_JGSSXDM,HJD_DZMC,HJD_RHYZBS,JZD_DZBM,JZD_XZQHDM,JZD_DZMC,ZJDZ_DZMC,HQYLDYYLBZ," +
                "DJZT,DJ_RQSJ,DJYY,JCDJ_RQSJ,HKDJID,TJYXZQH,CXSX,TB_BJZD,TB_XLBH,XXRKSJ,SXBZK_RKSJ --hbase-create-table --hbase-table GA_CZRKJBXX " +
                "--hbase-row-key GMSFHM --column-family info";

        System.out.println(sqoop_command);

        SqoopUtils.importDataUseSSH(sqoop_server_ip,sqoop_server_user,sqoop_command);

    }

}
