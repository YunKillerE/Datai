package tools;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * Created by yunchen on 2016/12/27.
 */
public class PropertiesUtils {

    public static String Get_Properties(String path, String name) {

        //ResourceBundle resource = ResourceBundle.getBundle("config.properties");//test为属性文件名，放在包com.mmq下，如果是放在src下，直接用test即可
        Properties p = null;

        p = System.getProperties();
        try {
            //p.load(new FileInputStream(new File("C:\\Users\\yunchen\\Desktop\\yunchen\\sqoop1_import\\src\\config.properties")));
            p.load(new FileInputStream(new File(path)));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return p.getProperty(name);

    }
}
