package com.nova.main;

import com.nova.utils.SqoopUtils;
import net.neoremind.sshxcute.exception.TaskExecFailException;

/**
 * Created by yunchen on 2017/4/6.
 */
public class HdfsExportMain {

    public static void main(String[] args) throws TaskExecFailException {

        //sqoop export --connect jdbc:oracle:thin:@192.168.1.226:1521:xe --table TT --username ROOT --password root --export-dir /dd
        // --columns 'ID,NAME' --input-fields-terminated-by ',' --input-lines-terminated-by '\n' -m 1 --update-key ID --update-mode allowinsert
        String jdbc_url = "jdbc:oracle:thin:@192.168.1.227:1521:xe";
        String table_name = "TT";
        String table_columns = "ID,NAME";
        String input_fields = ",";
        String update_key = "ID";

        String sqoop_ip = "192.168.1.225";
        String sqoop_user = "root";

        String sqoop_command = "source /etc/profile;sqoop export --connect jdbc:oracle:thin:@192.168.1.226:1521:xe --table TT --username ROOT --password root --export-dir /dd --columns 'ID,NAME' --input-fields-terminated-by ',' --input-lines-terminated-by '\\n' -m 1 --update-key ID --update-mode allowinsert";

        SqoopUtils.importDataUseSSH(sqoop_ip,sqoop_user,sqoop_command);

    }
}
